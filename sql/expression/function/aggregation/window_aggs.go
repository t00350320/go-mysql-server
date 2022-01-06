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
	"math"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var _ sql.WindowFunction = (*SumAgg)(nil)

type SumAgg struct {
	buf                          sql.WindowBuffer
	partitionStart, partitionEnd int
	prevInterval                 sql.WindowInterval
	expr                         sql.Expression
	prevSum                      int
}

func NewSumAgg(e sql.Expression) *SumAgg {
	return &SumAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (s *SumAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	s.partitionStart, s.partitionEnd = interval.Start, interval.End
	s.prevSum = 0
	s.prevInterval = sql.WindowInterval{}
	return nil
}

func (s *SumAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	return
}

func (s *SumAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	var res int
	for i := interval.Start; i < interval.End; i++ {
		val, _ := s.expr.Eval(sql.NewEmptyContext(), buf[i])
		res += val.(int)
	}
	return res
}

type MaxAgg struct {
	buf                          sql.WindowBuffer
	partitionStart, partitionEnd int
	prevInterval                 sql.WindowInterval
	expr                         sql.Expression
	max                          interface{}
}

func NewMaxAgg(e sql.Expression) *MaxAgg {
	return &MaxAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (m *MaxAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	m.partitionStart, m.partitionEnd = interval.Start, interval.End
	m.max = -math.MaxInt
	m.prevInterval = sql.WindowInterval{}
	return nil
}

func (m *MaxAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

func (m *MaxAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	var max interface{}
	for i := interval.Start; i < interval.End; i++ {
		row := buffer[i]
		v, err := m.expr.Eval(ctx, row)
		if err != nil {
			return err
		}

		if reflect.TypeOf(v) == nil {
			continue
		}

		if max == nil {
			max = v
		}

		cmp, err := m.expr.Type().Compare(v, max)
		if err != nil {
			return err
		}
		if cmp == 1 {
			max = v
		}
	}
	return max
}

var _ sql.WindowFunction = (*MaxAgg)(nil)

type LastAgg struct {
	buf                          sql.WindowBuffer
	partitionStart, partitionEnd int
	prevInterval                 sql.WindowInterval
	expr                         sql.Expression
}

func NewLastAgg(e sql.Expression) *LastAgg {
	return &LastAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (l *LastAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	l.partitionStart, l.partitionEnd = interval.Start, interval.End
	l.prevInterval = sql.WindowInterval{}
	return nil
}

func (l *LastAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

func (l *LastAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	row := buffer[interval.End-1]
	v, err := l.expr.Eval(ctx, row)
	if err != nil {
		return err
	}
	return v
}

var _ sql.WindowFunction = (*LastAgg)(nil)

type CountAgg struct {
	buf                          sql.WindowBuffer
	partitionStart, partitionEnd int
	prevInterval                 sql.WindowInterval
	expr                         sql.Expression
	prefixSum                    []int
}

func NewCountAgg(e sql.Expression) *CountAgg {
	return &CountAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (c *CountAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	c.partitionStart, c.partitionEnd = interval.Start, interval.End
	var err error
	c.prefixSum, err = c.computePrefixSum(ctx, interval, buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *CountAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

func (c *CountAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	startIdx := interval.Start - c.partitionStart - 1
	endIdx := interval.End - c.partitionStart - 1

	cnt := c.prefixSum[endIdx]
	if startIdx > 0 {
		cnt -= c.prefixSum[interval.Start-c.partitionStart-1]
	}
	return cnt
}

func (c *CountAgg) computePrefixSum(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) ([]int, error) {
	intervalLen := interval.End - interval.Start
	sums := make([]int, intervalLen)
	var last int
	for i := 0; i < intervalLen; i++ {
		row := buf[i]
		var inc bool
		if _, ok := c.expr.(*expression.Star); ok {
			inc = true
		} else {
			v, err := c.expr.Eval(ctx, row)
			if v != nil {
				inc = true
			}

			if err != nil {
				return nil, err
			}
		}

		if inc {
			last += 1
			sums[i] = last
			last = sums[i]
		} else {
			sums[i] = last
		}
	}
	return sums, nil
}

var _ sql.WindowFunction = (*LastAgg)(nil)
