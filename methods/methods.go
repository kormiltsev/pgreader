package methods

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	env "github.com/caarlos0/env/v6"
	pgx "github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

// Settingos is interface for operate with DB
type Settingos struct {
	DBlink string    `env:"DATABASE_URL"`
	DB     *pgx.Conn `env:"-"`
}

// NewConfig returns empty Settingos
func NewConfig() *Settingos {
	return &Settingos{}
}

func funWithFlags() string {
	DBconnectionlink := flag.String("d", "", "DB connection link")
	flag.Parse()
	return *DBconnectionlink
}

func PrintPostgressTablesInfo(ctx context.Context, dbURL ...string) {

	// 1.
	// connect and use during main
	// dbURL can be empty
	Config, err := NewConnect(ctx, dbURL...)
	if err != nil {
		log.Panic("cant connect to database: ", err)
		return
	}
	defer Config.CloseDB(ctx)

	// 2.A
	// tables qty
	qty, err := Config.GetTableQty(ctx)
	if err != nil {
		log.Println("cant count tables", err)
		return
	}
	fmt.Println(qty, " tables")

	// 2.B
	// GetDBsize
	dbSize, err := Config.GetDBsize(ctx)
	if err != nil {
		log.Println("cant get db size", err)
		return
	}
	fmt.Println("Total DB size: ", dbSize)

	// 2.C
	// tables names
	tableNames, err := Config.GetTablesNames(ctx)
	if err != nil {
		log.Println("cant get tables names", err)
		return
	}
	for i, nm := range tableNames {
		// 2.1.A
		// rows quantity by table
		rowsQty, err := Config.GetRowsQtyByTable(ctx, nm)
		if err != nil {
			log.Println("cant get rows qty: ", err)
			return
		}

		fmt.Printf("%d. %s: %d rows\n", i+1, nm, rowsQty)

		// 2.1.B
		// get columns names and types
		columnNameAndType, err := Config.GetColumnNamesAndTypesByTable(ctx, nm)
		if err != nil {
			log.Println("cant get column names: ", err)
			return
		}

		for j, nameAndType := range columnNameAndType {
			fmt.Printf(" - %d. %s (%s)\n", j+1, nameAndType[0], nameAndType[1])
		}
	}
}

func (con *Settingos) environment() {
	err := env.Parse(con)
	if err != nil {
		log.Fatal(err)
	}
	// log.Println("got from env:", con)
}

func (con *Settingos) gotoenvfile() {
	godotenv.Load()
	con.DBlink = os.Getenv("DATABASE_URL")
}

// PrintPostgressTablesMainInfo
// func PrintPostgressTablesMainInfo(ctx context.Context, dbURL ...string) {
// 	Config, err := NewConnect(ctx, dbURL...)
// 	if err != nil {
// 		log.Println("cant connect to database: ", err)
// 		return
// 	}
// 	defer Config.CloseDB(ctx)

// 	// tables names
// 	tableNames, err := Config.GetTablesNames(ctx)
// 	if err != nil {
// 		log.Println("cant get tables names", err)
// 		return
// 	}
// 	for i, nm := range tableNames {
// 		// rows quantity by table
// 		rowsQty, err := Config.GetRowsQtyByTable(ctx, nm)
// 		if err != nil {
// 			log.Println("cant get rows qty: ", err)
// 			return
// 		}

// 		fmt.Printf("%d. %s: %d rows\n", i+1, nm, rowsQty)

// 		// get columns names and types
// 		columnNameAndType, err := Config.GetColumnNamesAndTypesByTable(ctx, nm)
// 		if err != nil {
// 			log.Println("cant get column names: ", err)
// 			return
// 		}

// 		for j, nameAndType := range columnNameAndType {
// 			fmt.Printf(" - %d. %s (%s)\n", j+1, nameAndType[0], nameAndType[1])
// 		}
// 	}
// }

// NewConnect returns & on DB (interface). Accepts ctx and DATABASE_URL (optional). If no parameter DATABASE_URL is set, go ENV.
func NewConnect(ctx context.Context, dbURL ...string) (*Settingos, error) {
	con := NewConfig()

	if len(dbURL) != 0 {
		if dbURL[0] != "" {
			con.DBlink = dbURL[0]
		}
	} else {
		con.gotoenvfile()
		con.environment()
	}

	if con.DBlink == "" {
		return nil, fmt.Errorf("no database_url was found. use NewConnect(ctx, database_url)")
	}
	var err error
	con.DB, err = pgx.Connect(ctx, con.DBlink)
	if err != nil {
		return nil, err
	}

	_, err = con.DB.Exec(context.Background(), ";")
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return con, nil
}

// CloseDB is closing connection. Dont forget to close, use 'defer connection.CloseDB()'
func (con *Settingos) CloseDB(ctx context.Context) {
	con.DB.Close(ctx)
}

// GetTableQty returns quantity of tables in database
func (con *Settingos) GetTableQty(ctx context.Context) (int, error) {
	var sqlTableQty = `
	SELECT COUNT(table_name) FROM information_schema.tables
	WHERE table_schema NOT IN ('information_schema','pg_catalog')
;`
	var qty int
	err := con.DB.QueryRow(ctx, sqlTableQty).Scan(&qty)
	if err != nil {
		log.Printf("Unable to query: %s\n", err)
		return 0, err
	}
	return qty, nil
}

// GetTablesNames returns table names available in database
func (con *Settingos) GetTablesNames(ctx context.Context) ([]string, error) {
	var sqlTableNames = `
	SELECT table_name FROM information_schema.tables
	WHERE table_schema NOT IN ('information_schema','pg_catalog')
;`
	rows, err := con.DB.Query(ctx, sqlTableNames)
	if err != nil {
		log.Println("cant query for names: ", err)
		return nil, err
	}
	defer rows.Close()
	var rowNames = make([]string, 0)
	var nm string
	for rows.Next() {
		err := rows.Scan(&nm)
		if err != nil {
			log.Println("cant scan query for names: ", err)
			return nil, err
		}
		rowNames = append(rowNames, nm)
	}
	if err := rows.Err(); err != nil {
		log.Println("error in query: ", err)
		return nil, err
	}
	return rowNames, nil

}

// GetDBsize returns approx database size in text format
func (con *Settingos) GetDBsize(ctx context.Context) (string, error) {
	var sqlDBsize = `
	SELECT pg_size_pretty(pg_database_size(current_database()))
;`
	var size string
	err := con.DB.QueryRow(ctx, sqlDBsize).Scan(&size)
	if err != nil {
		log.Printf("Unable to query size: %s\n", err)
		return "", err
	}
	return size, nil
}

// GetRowsQtyByTable accepts ctx and table name; returns rows quantity and error.
func (con *Settingos) GetRowsQtyByTable(ctx context.Context, tableName string) (int, error) {
	var sqlDBsize = `
	SELECT COUNT(*) FROM ` + tableName + `
;`
	var rowsqty int
	err := con.DB.QueryRow(ctx, sqlDBsize).Scan(&rowsqty)
	if err != nil {
		log.Printf("Unable to query rows qty: %s\n", err)
		return 0, err
	}
	return rowsqty, nil
}

// GetColumnNamesAndTypesByTable returns array of column names+type by table name
func (con *Settingos) GetColumnNamesAndTypesByTable(ctx context.Context, tableName string) ([][]string, error) {
	var sqlColName = `
	SELECT column_name, data_type
	FROM information_schema.columns
   WHERE table_schema NOT IN ('information_schema','pg_catalog')
	 AND table_name   = '` + tableName + `'
	   ;`
	rows, err := con.DB.Query(ctx, sqlColName)
	if err != nil {
		log.Println("cant query for column names: ", err)
		return nil, err
	}
	defer rows.Close()
	var rowNames = make([][]string, 0)
	// var nt = make([]string, 2)
	var n, t string
	for rows.Next() {
		err := rows.Scan(&n, &t)
		if err != nil {
			log.Println("cant scan query for column names: ", err)
			return nil, err
		}
		rowNames = append(rowNames, []string{n, t})
	}
	if err := rows.Err(); err != nil {
		log.Println("error in query: ", err)
		return nil, err
	}
	return rowNames, nil
}
