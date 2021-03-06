package observation_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/dp-filter/observation/observationtest"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
)

var testContext = context.Background()

func TestStore_GetCSVRows(t *testing.T) {

	Convey("Given an store with a mock DB connection", t, func() {

		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30"}},
				{Name: "sex", Options: []string{"male", "female"}},
			},
		}

		expectedQuery := "MATCH (i:`_888_Instance`) RETURN i.header as row " +
			"UNION ALL " +
			"MATCH (o)-[:isValueOf]->(`age`:`_888_age`), (o)-[:isValueOf]->(`sex`:`_888_sex`) " +
			"WHERE (`age`.value='29' OR `age`.value='30') " +
			"AND (`sex`.value='male' OR `sex`.value='female') " +
			"RETURN o.value AS row"

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow}, nil, nil
			},
		}

		mockedDBConnection := &observationtest.ConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (bolt.Rows, error) {
				return mockBoltRows, nil
			},
		}

		mockedPool := &observationtest.DBPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return mockedDBConnection, nil
			},
		}

		store := observation.NewStore(mockedPool)

		Convey("When GetCSVRows is called without a limit", func() {

			rowReader, err := store.GetCSVRows(testContext, filter, nil)

			Convey("The expected query is sent to the database", func() {

				actualQuery := mockedDBConnection.QueryNeoCalls()[0].Query

				So(len(mockedDBConnection.QueryNeoCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery)
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})

		Convey("When GetCSVRows is called with a limit of 20", func() {

			limitRows := 20
			rowReader, err := store.GetCSVRows(testContext, filter, &limitRows)

			Convey("The expected query is sent to the database", func() {

				actualQuery := mockedDBConnection.QueryNeoCalls()[0].Query

				So(len(mockedDBConnection.QueryNeoCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery+" LIMIT 20")
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})
	})
}

func TestStore_GetCSVRowsEmptyFilter(t *testing.T) {
	filterID := "1234567890"
	InstanceID := "0987654321"

	expectedQuery := fmt.Sprintf("MATCH (i:`_%s_Instance`) RETURN i.header as row UNION ALL "+
		"MATCH(o: `_%s_observation`) return o.value as row", InstanceID, InstanceID)

	Convey("Given valid database connection", t, func() {

		expectedCSVRowHeader := "the,csv,row"
		expectedCSVRowData := "1,2,3"

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRowHeader, expectedCSVRowData}, nil, nil
			},
		}

		mockedDBConnection := &observationtest.ConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (bolt.Rows, error) {
				return mockBoltRows, nil
			},
		}

		mockedPool := &observationtest.DBPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return mockedDBConnection, nil
			},
		}

		store := observation.NewStore(mockedPool)

		Convey("When GetCSVRows is called a filter with nil dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:         filterID,
				InstanceID:       InstanceID,
				DimensionFilters: nil,
			}

			result, err := store.GetCSVRows(testContext, filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(mockedDBConnection, expectedQuery)
		})

		Convey("When GetCSVRows is called a filter with empty dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:         filterID,
				InstanceID:       InstanceID,
				DimensionFilters: []*observation.DimensionFilter{},
			}

			result, err := store.GetCSVRows(testContext, filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(mockedDBConnection, expectedQuery)
		})

		Convey("When GetCSVRows is called a filter with a list of empty dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:   filterID,
				InstanceID: InstanceID,
				DimensionFilters: []*observation.DimensionFilter{
					&observation.DimensionFilter{
						Name:    "",
						Options: []string{},
					},
				},
			}

			result, err := store.GetCSVRows(testContext, filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(mockedDBConnection, expectedQuery)
		})
	})
}

func TestStore_GetCSVRowsDimensionEmpty(t *testing.T) {

	Convey("Given an store with a mock DB connection", t, func() {

		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30"}},
				{Name: "sex", Options: []string{}},
			},
		}

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow}, nil, nil
			},
		}

		mockedDBConnection := &observationtest.ConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (bolt.Rows, error) {
				return mockBoltRows, nil
			},
		}

		mockedPool := &observationtest.DBPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return mockedDBConnection, nil
			},
		}

		store := observation.NewStore(mockedPool)

		Convey("When GetCSVRows is called with a filter with an empty dimension options and no limit", func() {

			expectedQuery := "MATCH (i:`_888_Instance`) RETURN i.header as row " +
				"UNION ALL " +
				"MATCH (o)-[:isValueOf]->(`age`:`_888_age`) " +
				"WHERE (`age`.value='29' OR `age`.value='30') " +
				"RETURN o.value AS row"

			rowReader, err := store.GetCSVRows(testContext, filter, nil)

			Convey("Then the expected query is sent to the database", func() {

				actualQuery := mockedDBConnection.QueryNeoCalls()[0].Query

				So(len(mockedDBConnection.QueryNeoCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery)
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})
	})
}

func assertEmptyFilterResults(reader observation.CSVRowReader, expectedCSVRow string, err error) {
	Convey("The expected result is returned with no error", func() {
		So(err, ShouldBeNil)
		row, _ := reader.Read()
		So(row, ShouldEqual, expectedCSVRow+"\n")
	})
}

func assertEmptyFilterQueryInvocations(connection *observationtest.ConnMock, expectedQuery string) {
	Convey("Then the expected query is sent to the database one time", func() {
		So(len(connection.QueryNeoCalls()), ShouldEqual, 1)
		So(connection.QueryNeoCalls()[0].Query, ShouldEqual, expectedQuery)
	})

}
