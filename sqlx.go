package sqlx

import "context"

// Select using this Conn.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	r := db.QueryRow(ctx, query, args...)
	return r.scanAny(dest, false)
}
