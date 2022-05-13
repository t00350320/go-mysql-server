package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
)

var NullRangeTests = []QueryTest{
	{
		Query: "select * from null_ranges where y IS NULL or y < 1",
		Expected: []sql.Row{
			{0, 0},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query:    "select * from null_ranges where y IS NULL and y < 1",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y IS NOT NULL",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NOT NULL",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y = 0 or y = 1",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y < 1 or y > 1",
		Expected: []sql.Row{
			{0, 0},
			{2, 2},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NOT NULL and x > 1",
		Expected: []sql.Row{
			{2, 2},
		},
	}, {
		Query: "select * from null_ranges where y IS NULL and x = 4",
		Expected: []sql.Row{
			{4, nil},
		},
	}, {
		Query: "select * from null_ranges where y IS NULL and x > 1",
		Expected: []sql.Row{
			{3, nil},
			{4, nil},
		},
	},
	{
		Query:    "select * from null_ranges where y IS NULL and y IS NOT NULL",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from null_ranges where y is NULL and y > -1 and y > -2",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from null_ranges where y > -1 and y < 7 and y IS NULL",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from null_ranges where y > -1 and y > -2 and y IS NOT NULL",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y > -1 and y > 1 and y IS NOT NULL",
		Expected: []sql.Row{
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y < 6 and y > -1 and y IS NOT NULL",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
}
