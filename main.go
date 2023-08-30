/*
pgreader connect to postgres and print main data:
* quantity of tables
* Total size of BD (approximately)
* list of tables names with quantity of rows
  - list of rows' names with data type

Requires database URL as flag -d="postgres://postgres:root@127.0.0.1:5432".
Witout flag connector will parse:
- ENV "DATABASE_URL" and if no this variable go to .env-file
- .env "DATABASE_URL"

Output like this:

14  tables
Total DB size:  30 MB
1. baserows: 27 rows
  - 1. id (bigint)
  - 2. surl (text)
  - 3. lurl (text)

2. itemkeeper_users: 2 rows
  - 1. id (integer)
  - 2. login (character varying)
  - 3. password (character varying)
  - 4. userid (text)
  - 5. created_at (timestamp with time zone)

...
*/
package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/kormiltsev/pgreader/methods"
)

func funWithFlags() string {
	DBconnectionlink := flag.String("d", "", "DB connection link")
	flag.Parse()
	return *DBconnectionlink
}

func main() {
	ctx := context.Background()
	methods.PrintPostgressTablesInfo(ctx, funWithFlags()) // if there is no flag provided, ENV and .env will be parsed
	// or put URL here:
	// methods.PrintPostgressTablesInfo(ctx, "postgres://postgres:root@127.0.0.1:5432")
	// or without URL. Then URL will be parsed from ENV or .env file "DATABASE_URL"
	// methods.PrintPostgressTablesInfo(ctx)
}

// Other way to use pgparser:
func do(ctx context.Context, URL string) {
	// get new configs with URL set
	configs, err := methods.NewConnect(ctx, URL)
	if err != nil {
		panic(err)
	}
	defer configs.CloseDB(ctx)

	// Tables quantity
	tablesQuantity, err := configs.GetTableQty(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(tablesQuantity)

	// List of tables names
	tablesNames, err := configs.GetTablesNames(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(tablesNames) // []string

	// DB's size approximately
	size, err := configs.GetDBsize(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(size) // string

	// rows quantity in this table
	rowsQuantity, err := configs.GetRowsQtyByTable(ctx, tablesNames[0])
	if err != nil {
		panic(err)
	}
	fmt.Println(rowsQuantity) // int

	// list of rows' names in this table
	listOfRowsNamesWithType, err := configs.GetColumnNamesAndTypesByTable(ctx, tablesNames[0])
	if err != nil {
		panic(err)
	}
	fmt.Println(listOfRowsNamesWithType) // [][]string

}
