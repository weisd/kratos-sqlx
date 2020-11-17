package sqlx

import (
	"context"
	sql "database/sql"

	sqlk "github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/weisd/kratos-sqlx/reflectx"
)

// Stmt is an sqlx wrapper around sql.Stmt with extra functionality
type Stmt struct {
	*sqlk.Stmt
	unsafe bool
	Mapper *reflectx.Mapper
}

// Close closes the statement.
func (s *Stmt) Close() (err error) {
	return s.Stmt.Close()
}

// Exec executes a prepared statement with the given arguments and returns a
// Result summarizing the effect of the statement.
func (s *Stmt) Exec(c context.Context, args ...interface{}) (res sql.Result, err error) {
	return s.Stmt.Exec(c, args)

}

// Query executes a prepared query statement with the given arguments and
// returns the query results as a *Rows.
func (s *Stmt) Query(c context.Context, args ...interface{}) (rows *Rows, err error) {
	rs, err := s.Stmt.Query(c, args)
	if err != nil {
		return nil, err
	}

	return &Rows{Rows: rs, unsafe: s.unsafe, Mapper: s.Mapper}, err
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
func (s *Stmt) QueryRow(c context.Context, args ...interface{}) (row *Row) {
	r, err := s.Stmt.Query(c, args)

	return &Row{Rows: r, err: err, unsafe: s.unsafe, Mapper: s.Mapper}
}
