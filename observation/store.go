package observation

import (
	"bytes"
	"fmt"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"strconv"
)

//go:generate moq -out observationtest/db_connection.go -pkg observationtest . DBConnection

// Store represents storage for observation data.
type Store struct {
	dBConnection DBConnection
}

// DBConnection provides a connection to the database.
type DBConnection interface {
	QueryNeo(query string, params map[string]interface{}) (bolt.Rows, error)
}

// NewStore returns a new store instace using the given DB connection.
func NewStore(dBConnection DBConnection) *Store {
	return &Store{
		dBConnection: dBConnection,
	}
}

// GetCSVRows returns a reader allowing individual CSV rows to be read. Rows returned
// can be limited, to stop this pass in nil.
func (store *Store) GetCSVRows(filter *Filter, limit *int) (CSVRowReader, error) {

	headerRowQuery := fmt.Sprintf("MATCH (i:`_%s_Instance`) RETURN i.header as row", filter.InstanceID)

	unionQuery := headerRowQuery + " UNION ALL " + createObservationQuery(filter)

	if limit != nil {
		limitAsString := strconv.Itoa(*limit)
		unionQuery += " LIMIT " + limitAsString
	}

	log.Info("neo4j query", log.Data{
		"filterID":   filter.FilterID,
		"instanceID": filter.InstanceID,
		"query":      unionQuery,
	})
	rows, err := store.dBConnection.QueryNeo(unionQuery, nil)
	if err != nil {
		return nil, err
	}

	return NewBoltRowReader(rows), nil
}

func createObservationQuery(filter *Filter) string {
	if filter.IsEmpty() {
		// if no dimension filter are specified than match all observations
		log.Info("no dimension filters supplied, generating entire dataset query", log.Data{
			"filterID":   filter.FilterID,
			"instanceID": filter.InstanceID,
		})
		return fmt.Sprintf("MATCH(o: `_%s_observation`) return o.value as row", filter.InstanceID)
	}

	matchDimensions := "MATCH "
	where := " WHERE "
	with := " WITH "
	match := " MATCH "

	for index, dimension := range filter.DimensionFilters {

		if index != 0 {
			matchDimensions += ", "
			where += " AND "
			with += ", "
			match += ", "
		}

		optionList := createOptionList(dimension.Options)
		matchDimensions += fmt.Sprintf("(%s:`_%s_%s`)", dimension.Name, filter.InstanceID, dimension.Name)
		where += fmt.Sprintf("%s.value IN [%s]", dimension.Name, optionList)
		with += dimension.Name
		match += fmt.Sprintf("(o:`_%s_observation`)-[:isValueOf]->(%s)", filter.InstanceID, dimension.Name)
	}

	return matchDimensions + where + with + match + " RETURN o.value AS row"
}

func createOptionList(options []string) string {

	var buffer bytes.Buffer

	for index, option := range options {

		if index != 0 {
			buffer.WriteString(", ")
		}

		buffer.WriteString(fmt.Sprintf("'%s'", option))
	}

	return buffer.String()
}
