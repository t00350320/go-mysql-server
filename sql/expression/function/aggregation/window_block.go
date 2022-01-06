// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"io"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type Aggregation struct {
	fn     sql.WindowFunction
	framer sql.WindowFramer
}

func NewAggregation(a sql.WindowFunction, f sql.WindowFramer) Aggregation {
	return Aggregation{fn: a, framer: f}
}

// WindowBlock is a set of aggregations for a specific
// partition key and sort key set (WPK, WSK). The block of
// aggs will share a sql.WindowBuffer.
//
type WindowBlock struct {
	PartitionBy []sql.Expression
	SortBy      sql.SortFields
	unordered   bool

	aggs []Aggregation

	input      sql.WindowBuffer
	output     sql.WindowBuffer
	outputIdx  []int
	partitions []sql.WindowInterval
}

func NewWindowBlock(partitionBy []sql.Expression, sortBy sql.SortFields, aggStates []Aggregation) *WindowBlock {
	return &WindowBlock{
		PartitionBy: partitionBy,
		SortBy:      sortBy,
		aggs:        aggStates,
	}
}

//func (w *WindowBlock) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
//	return newWindowBlockIter(w.PartitionBy, w.SortBy, w.aggs, w.)
//}

type windowBlockIter struct {
	ctx   *sql.Context
	pos   int
	child sql.RowIter

	partitionBy []sql.Expression
	sortBy      sql.SortFields
	aggs        []Aggregation
	groupBy     bool

	input, output    sql.WindowBuffer
	outputIdx        []int
	partitions       []sql.WindowInterval
	currentPartition sql.WindowInterval
	partitionIdx     int
	done             bool
}

var _ sql.RowIter = (*windowBlockIter)(nil)

func NewWindowBlockIter(partitionBy []sql.Expression, sortBy sql.SortFields, aggs []Aggregation, child sql.RowIter) *windowBlockIter {
	return &windowBlockIter{
		partitionBy: partitionBy,
		sortBy:      sortBy,
		aggs:        aggs,
		child:       child,
	}
}

func (i *windowBlockIter) SetGroupBy() {
	i.groupBy = true
}

func (i *windowBlockIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	if i.input == nil {
		err := i.initializeBuffers(ctx)
		if err != nil {
			return nil, err
		}
	}

	if i.currentPartition.Start == i.pos {
		for _, a := range i.aggs {
			err := a.fn.StartPartition(ctx, i.currentPartition, i.input)
			if err != nil {
				return nil, err
			}
			a.framer.StartPartition(i.currentPartition)
		}
	}

	var row = make(sql.Row, len(i.aggs))
	for j, agg := range i.aggs {
		interval, _ := agg.framer.Next()
		row[j] = agg.fn.Compute(ctx, interval, i.input)
	}

	i.incrPos()
	return row, nil
}

func (i *windowBlockIter) incrPos() {
	if i.groupBy {
		i.pos = i.currentPartition.End - 1
	}

	i.pos++
	if i.pos < i.currentPartition.End {
		return
	}
	i.partitionIdx++
	if i.partitionIdx > len(i.partitions)-1 {
		i.done = true
		return
	}
	i.currentPartition = i.partitions[i.partitionIdx]
	return
}

func (i *windowBlockIter) Close(ctx *sql.Context) error {
	i.input = nil
	return i.child.Close(ctx)
}

// initializeBuffers sorts the buffer by (WPK, WSK)
// if the block is not unordered
func (i *windowBlockIter) initializeBuffers(ctx *sql.Context) error {
	// collect rows
	i.input = make(sql.WindowBuffer, 0)
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		i.input = append(i.input, row)
	}

	// sort all rows by partition
	sorter := &expression.Sorter{
		SortFields: append(partitionsToSortFields(i.partitionBy), i.sortBy...),
		Rows:       i.input,
		Ctx:        ctx,
	}
	sort.Stable(sorter)

	// tally partitions
	i.partitions = make([]sql.WindowInterval, 0)
	startIdx := 0
	var lastRow sql.Row
	for j, row := range i.input {
		ok, err := isNewPartition(ctx, i.partitionBy, lastRow, row)
		if err != nil {

		}
		if ok && j > startIdx {
			i.partitions = append(i.partitions, sql.WindowInterval{Start: startIdx, End: j})
			startIdx = j
		}
		lastRow = row
	}

	if startIdx < len(i.input) {
		i.partitions = append(i.partitions, sql.WindowInterval{Start: startIdx, End: len(i.input)})
	}

	if len(i.partitions) == 0 {
		return io.EOF
	}

	i.partitionIdx = 0
	i.currentPartition = i.partitions[0]

	return nil
}

func partitionsToSortFields(partitionExprs []sql.Expression) sql.SortFields {
	sfs := make(sql.SortFields, len(partitionExprs))
	for i, expr := range partitionExprs {
		sfs[i] = sql.SortField{
			Column: expr,
			Order:  sql.Ascending,
		}
	}
	return sfs
}

func isNewPartition(ctx *sql.Context, partitionBy []sql.Expression, last sql.Row, row sql.Row) (bool, error) {
	if len(last) == 0 {
		return true, nil
	}

	if len(partitionBy) == 0 {
		return false, nil
	}

	lastExp, _, err := evalExprs(ctx, partitionBy, last)
	if err != nil {
		return false, err
	}

	thisExp, _, err := evalExprs(ctx, partitionBy, row)
	if err != nil {
		return false, err
	}

	for i := range lastExp {
		if lastExp[i] != thisExp[i] {
			return true, nil
		}
	}

	return false, nil
}
