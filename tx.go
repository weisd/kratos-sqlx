package sqlx

import (
	sql "database/sql"
	"fmt"
	"reflect"

	sqlk "github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/go-kratos/kratos/pkg/database/sqlx/reflectx"
)

// Tx is an sqlx wrapper around sql.Tx with extra functionality
type Tx struct {
	*sqlk.Tx
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() (err error) {
	return tx.Tx.Rollback()
}

// Exec runs Exec within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.Tx.Exec(query, args...)
}

// Query executes a query that returns rows, typically a SELECT.
func (tx *Tx) Query(query string, args ...interface{}) (rows *Rows, err error) {
	r, err := tx.Tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	row, err := tx.Tx.Query(query, args...)
	return &Row{Rows: row, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (tx *Tx) Stmt(stmt interface{}) *Stmt {
	var s *sqlk.Stmt
	switch v := stmt.(type) {
	case Stmt:
		s = v.Stmt
	case *Stmt:
		s = v.Stmt
	case *sqlk.Stmt:
		s = v
	default:
		panic(fmt.Sprintf("non-statement type %v passed to Stmtx", reflect.ValueOf(stmt).Type()))
	}
	return &Stmt{Stmt: tx.Tx.Stmt(s), Mapper: tx.Mapper}
}

// Prepare  a statement within a transaction.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	s, err := tx.Tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{Stmt: s, unsafe: isUnsafe(tx), Mapper: mapperFor(tx)}, err
}
