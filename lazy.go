package lazy

import (
	"context"
	"database/sql"
)

// Stmt is a prepared statement.
type Stmt struct {
	db    *sql.DB
	stmt  *sql.Stmt
	query string
}

// Prepare creates a prepared statement for later queries or executions.
func Prepare(db *sql.DB, query string) *Stmt {
	stmt, err := db.Prepare(query)
	if err != nil {
		stmt = nil
	}
	return &Stmt{
		db:    db,
		stmt:  stmt,
		query: query,
	}
}

// Exec executes a prepared statement with the given arguments.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	if s.Stmt() != nil {
		return s.stmt.Exec(args...)
	}
	return s.db.Exec(s.query, args...)
}

// ExecContext executes a prepared statement with the given arguments.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	if s.Stmt() != nil {
		return s.stmt.ExecContext(ctx, args...)
	}
	return s.db.ExecContext(ctx, s.query, args...)
}

// Query executes a prepared query statement with the given arguments.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	if s.Stmt() != nil {
		return s.stmt.Query(args...)
	}
	return s.db.Query(s.query, args...)
}

// QueryContext executes a prepared query statement with the given arguments.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	if s.Stmt() != nil {
		return s.stmt.QueryContext(ctx, args...)
	}
	return s.db.QueryContext(ctx, s.query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	if s.Stmt() != nil {
		return s.stmt.QueryRow(args...)
	}
	return s.db.QueryRow(s.query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// It tries to create the statement if it has not been created.
// It executes a query directly if the creation has failed.
func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	if s.Stmt() != nil {
		return s.stmt.QueryRowContext(ctx, args...)
	}
	return s.db.QueryRowContext(ctx, s.query, args...)
}

// Stmt returns a prepared statement. It tries to create the statement if
// it has not been created.
func (s *Stmt) Stmt() *sql.Stmt {
	if s.stmt != nil {
		return s.stmt
	}
	stmt, err := s.db.Prepare(s.query)
	if err != nil {
		return nil
	}
	s.stmt = stmt
	return s.stmt
}

// Raw returns a query string
func (s *Stmt) Raw() string {
	return s.query
}
