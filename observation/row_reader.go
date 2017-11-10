package observation

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
)

//go:generate moq -out observationtest/bolt_rows.go -pkg observationtest . BoltRows
//go:generate moq -out observationtest/row_reader.go -pkg observationtest . CSVRowReader

// BoltRows provides an interface to each row of results returned from the database.
type BoltRows bolt.Rows

// CSVRowReader provides a reader of individual rows (lines) of a CSV file.
type CSVRowReader interface {
	Read() (string, error)
	Close() error
}

// BoltRowReader translates Neo4j rows to CSV rows.
type BoltRowReader struct {
	rows BoltRows
}

// NewBoltRowReader returns a new reader instace for the given bolt rows.
func NewBoltRowReader(rows BoltRows) *BoltRowReader {
	return &BoltRowReader{
		rows: rows,
	}
}

// ErrNoDataReturned is returned if a Neo4j row has no data.
var ErrNoDataReturned = errors.New("no data returned in this row")

// ErrUnrecognisedType is returned if a Neo4j row does not have the expected string value.
var ErrUnrecognisedType = errors.New("the value returned was not a string")

// Read the next row, or return io.EOF
func (reader *BoltRowReader) Read() (string, error) {
	data, _, err := reader.rows.NextNeo()
	if err != nil {
		return "", err
	}

	if len(data) < 1 {
		return "", ErrNoDataReturned
	}

	if csvRow, ok := data[0].(string); ok {
		return csvRow + "\n", nil
	}

	return "", ErrUnrecognisedType
}

// Close the reader.
func (reader *BoltRowReader) Close() error {
	return reader.rows.Close()
}
