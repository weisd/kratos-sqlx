package sqlx

import (
	"context"
	sql "database/sql"

	sqlk "github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/weisd/kratos-sqlx/reflectx"
)

// DB DB
type DB struct {
	*sqlk.DB
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a database
// name and connection information.
func Open(c *sqlk.Config) (*DB, error) {
	db, err := sqlk.Open(c)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	err = db.Ping(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &DB{DB: db, driverName: "mysql", Mapper: mapper()}, err
}

// Begin begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	tx, err := db.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Exec execs the query using e
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.DB.Exec(ctx, query, args...)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(query string) (*Stmt, error) {
	s, err := db.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{Stmt: s, unsafe: isUnsafe(db), Mapper: mapperFor(db)}, err
}

// Prepared creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepared(query string) (stmt *Stmt) {
	s := db.DB.Prepared(query)
	return &Stmt{Stmt: s, unsafe: isUnsafe(db), Mapper: mapperFor(db)}
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	r, err := db.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// QueryRow executes a query that is expected to return at most one row. QueryRow always returns a non-nil value. Errors are deferred until Row's Scan method is called. If the query selects no rows, the *Row's Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the first selected row and discards the rest.
func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	row, err := db.DB.Query(ctx, query, args...)
	return &Row{Rows: row, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
}

// Close closes the write and read database, releasing any open resources.
func (db *DB) Close() (err error) {
	return db.DB.Close()
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping(c context.Context) (err error) {
	return db.DB.Ping(c)
}

// Master return *DB instance direct use master conn
// use this *DB instance only when you have some reason need to get result without any delay.
func (db *DB) Master() *DB {
	d := db.DB.Master()
	return &DB{DB: d, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}
}

// Unsafe returns a version of DB which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
// sqlx.Stmt and sqlx.Tx which are created from this DB will inherit its
// safety behavior.
func (db *DB) Unsafe() *DB {
	return &DB{DB: db.DB, driverName: db.driverName, unsafe: true, Mapper: db.Mapper}
}
