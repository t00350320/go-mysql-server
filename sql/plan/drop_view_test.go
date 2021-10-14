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

package plan

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

// Generates a database with a single table called mytable and a catalog with
// the view that is also returned. The context returned is the one used to
// create the view.
func mockData(t *testing.T) (sql.Database, *sql.Context, *sql.View) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int32},
		{Name: "s", Source: "mytable", Type: sql.Text},
	})

	db := memory.NewViewlessDatabase("db")
	db.AddTable("db", table)

	subqueryAlias := NewSubqueryAlias("myview", "select i",
		NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Int32, table.Name(), "i", true),
			},
			NewUnresolvedTable("dual", ""),
		),
	)

	createView := NewCreateView(db, subqueryAlias.Name(), nil, subqueryAlias, false)

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))

	_, err := createView.RowIter(ctx, nil)
	require.NoError(t, err)

	return db, ctx, createView.View()
}

// Tests that DropView works as expected and that the view is dropped in
// the catalog when RowIter is called, regardless of the value of ifExists
func TestDropExistingViewFromRegistry(t *testing.T) {
	test := func(ifExists bool) {
		db, ctx, view := mockData(t)

		singleDropView := NewSingleDropView(db, view.Name())
		dropView := NewDropView([]sql.Node{singleDropView}, ifExists)

		_, err := dropView.RowIter(ctx, nil)
		require.NoError(t, err)

		require.False(t, ctx.Exists(db.Name(), view.Name()))
	}

	test(false)
	test(true)
}

// Tests that DropView errors when trying to delete a non-existing view if and
// only if the flag ifExists is set to false
func TestDropNonExistingViewFromRegistry(t *testing.T) {
	test := func(ifExists bool) error {
		db, ctx, view := mockData(t)

		singleDropView := NewSingleDropView(db, "non-existing-view")
		dropView := NewDropView([]sql.Node{singleDropView}, ifExists)

		_, err := dropView.RowIter(ctx, nil)

		require.True(t, ctx.Exists(db.Name(), view.Name()))

		return err
	}

	err := test(true)
	require.NoError(t, err)

	err = test(false)
	require.Error(t, err)
}
