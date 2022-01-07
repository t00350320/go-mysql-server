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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestAggFuncs(t *testing.T) {
	tests := []struct {
		Name     string
		Agg      sql.WindowFunction
		Expected sql.Row
	}{
		{
			Name:     "count star",
			Agg:      NewCountAgg(expression.NewStar()),
			Expected: sql.Row{},
		},
		{
			Name:     "count without nulls",
			Agg:      NewCountAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count with nulls",
			Agg:      NewCountAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count distinct",
			Agg:      NewCountDistinctAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count distinct with nulls",
			Agg:      NewCountDistinctAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "max",
			Agg:      NewMaxAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "min",
			Agg:      NewMinAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "avg",
			Agg:      NewAvgAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "sum nulls",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "sum ints",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "sum int64",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "sum float64",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "first",
			Agg:      NewFirstAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "last",
			Agg:      NewLastAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
	}
	//create input buffer
	buf := []sql.Row{
		{1, 1, int64(1), float64(1), 1},
		{nil, 2, int64(2), float64(2), 1},
		{3, 3, int64(3), float64(3), 1},
		{4, 4, int64(4), float64(4), 1},
		{1, 1, int64(1), float64(1), 2},
		{nil, 2, int64(2), float64(2), 2},
		{3, 3, int64(3), float64(3), 2},
		{4, 4, int64(4), float64(4), 2},
		{1, 1, int64(1), float64(1), 3},
		{2, 2, int64(2), float64(2), 3},
		{nil, 3, int64(3), float64(3), 3},
		{nil, 4, int64(4), float64(4), 3},
		{5, 5, int64(5), float64(5), 3},
		{6, 6, int64(6), float64(6), 3},
	}

	partitions := []sql.WindowInterval{
		{Start: 0, End: 4},
		{Start: 4, End: 8},
		{Start: 8, End: 14},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			res := make(sql.Row, len(partitions))
			for i, p := range partitions {
				err := tt.Agg.StartPartition(ctx, p, buf)
				require.NoError(t, err)
				res[i] = tt.Agg.Compute(ctx, p, buf)
			}
			require.Equal(t, tt.Expected, res)
		})
	}
}

func TestSumAgg(t *testing.T) {
	tests := []struct {
		Name     string
		Agg      sql.WindowFunction
		Expected sql.Row
	}{
		{
			Name:     "count star",
			Agg:      NewCountAgg(expression.NewStar()),
			Expected: sql.Row{},
		},
		{
			Name:     "count without nulls",
			Agg:      NewCountAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count with nulls",
			Agg:      NewCountAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count distinct",
			Agg:      NewCountDistinctAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "count distinct with nulls",
			Agg:      NewCountDistinctAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "max",
			Agg:      NewMaxAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "min",
			Agg:      NewMinAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "avg",
			Agg:      NewAvgAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "sum",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "first",
			Agg:      NewFirstAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
		{
			Name:     "last",
			Agg:      NewLastAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{},
		},
	}
	//create input buffer
	buf := []sql.Row{
		{1, 1, 1},
		{nil, 2, 1},
		{3, 3, 1},
		{4, 4, 1},
		{1, 1, 2},
		{nil, 2, 2},
		{3, 3, 2},
		{4, 4, 2},
		{1, 1, 3},
		{2, 2, 3},
		{nil, 3, 3},
		{nil, 4, 3},
		{5, 5, 3},
		{6, 6, 3},
	}

	partitions := []sql.WindowInterval{
		{Start: 0, End: 4},
		{Start: 4, End: 8},
		{Start: 8, End: 14},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			res := make(sql.Row, len(partitions))
			for i, p := range partitions {
				err := tt.Agg.StartPartition(ctx, p, buf)
				require.NoError(t, err)
				res[i] = tt.Agg.Compute(ctx, p, buf)
			}
			require.Equal(t, tt.Expected, res)
		})
	}
}
