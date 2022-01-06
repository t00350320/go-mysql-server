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

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.WindowFramer = (*RowFramer)(nil)

type RowFramer struct {
	idx                          int
	partitionStart, partitionEnd int

	followOffset, precOffset int
	frameStart, frameEnd     int
	frameSet                 bool
}

func (r *RowFramer) Close() {
	panic("implement me")
}

func NewRowsFramer(followOffset, precOffset int) RowFramer {
	return RowFramer{
		followOffset:   followOffset,
		precOffset:     precOffset,
		frameEnd:       -1,
		frameStart:     -1,
		partitionStart: -1,
		partitionEnd:   -1,
	}
}

func (r *RowFramer) StartPartition(part sql.WindowInterval) {
	r.idx = part.Start
	r.partitionStart = part.Start
	r.partitionEnd = part.End
	r.frameStart = -1
	r.frameEnd = -1
	r.frameSet = false
}

func (r *RowFramer) Next() (sql.WindowInterval, error) {
	if r.idx > r.partitionEnd {
		return sql.WindowInterval{}, io.EOF
	}

	r.frameSet = false
	defer func() {
		r.frameSet = true
		r.idx++
	}()

	newStart := r.idx - r.precOffset
	if newStart < r.partitionStart {
		newStart = r.partitionStart
	}

	newEnd := r.idx + r.followOffset + 1
	if newEnd > r.partitionEnd {
		newEnd = r.partitionEnd
	}

	r.frameStart = newStart
	r.frameEnd = newEnd

	return r.Interval()
}

func (r *RowFramer) FirstIdx() int {
	return r.frameEnd
}

func (r *RowFramer) LastIdx() int {
	return r.frameStart
}

func (r *RowFramer) Interval() (sql.WindowInterval, error) {
	return sql.WindowInterval{Start: r.frameStart, End: r.frameEnd}, nil
}

func (r *RowFramer) SlidingInterval(ctx sql.Context) (sql.WindowInterval, sql.WindowInterval, sql.WindowInterval) {
	panic("implement me")
}

type PartitionFramer struct {
	idx                          int
	partitionStart, partitionEnd int

	followOffset, precOffset int
	frameStart, frameEnd     int
	frameSet                 bool
}

func NewPartitionFramer() *PartitionFramer {
	return &PartitionFramer{
		frameEnd:       -1,
		frameStart:     -1,
		partitionStart: -1,
		partitionEnd:   -1,
	}
}

func (p *PartitionFramer) StartPartition(part sql.WindowInterval) {
	p.idx = part.Start
	p.partitionStart = part.Start
	p.partitionEnd = part.End
	p.frameStart = part.Start
	p.frameEnd = part.End
	p.frameSet = true
}

func (p *PartitionFramer) Next() (sql.WindowInterval, error) {
	if p.idx > p.partitionEnd {
		return sql.WindowInterval{}, io.EOF
	}

	defer func() {
		p.idx++
	}()

	return p.Interval()
}

func (p *PartitionFramer) FirstIdx() int {
	return p.frameStart
}

func (p *PartitionFramer) LastIdx() int {
	return p.frameEnd
}

func (p *PartitionFramer) Interval() (sql.WindowInterval, error) {
	return sql.WindowInterval{Start: p.frameStart, End: p.frameEnd}, nil
}

func (p *PartitionFramer) SlidingInterval(ctx sql.Context) (sql.WindowInterval, sql.WindowInterval, sql.WindowInterval) {
	panic("implement me")
}

func (p *PartitionFramer) Close() {
	panic("implement me")
}

var _ sql.WindowFramer = (*PartitionFramer)(nil)
