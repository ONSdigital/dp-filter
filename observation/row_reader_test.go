package observation_test

import (
	"io"
	"testing"

	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/dp-filter/observation/observationtest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBoltRowReader_Read(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader", t, func() {

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow, "1,2,3"}, nil, nil
			},
		}

		mockConnection := &observationtest.DBConnectionMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows, mockConnection)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected csv row is returned", func() {
				So(err, ShouldBeNil)
				So(row, ShouldEqual, expectedCSVRow+"\n")
			})
		})
	})
}

func TestBoltRowReader_ReadError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns io.EOF", t, func() {

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return nil, nil, io.EOF
			},
		}

		mockConnection := &observationtest.DBConnectionMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows, mockConnection)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The error from the Bolt reader is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrNoInstanceFound)
				So(row, ShouldEqual, "")
			})
		})
	})
}

func TestBoltRowReader_Read_NoDataError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns a row with no data.", t, func() {

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{}, nil, nil
			},
		}

		mockConnection := &observationtest.DBConnectionMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows, mockConnection)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrNoDataReturned)
				So(row, ShouldEqual, "")
			})
		})
	})
}

func TestBoltRowReader_Read_TypeError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns a row with no data.", t, func() {

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{666, 666}, nil, nil
			},
		}

		mockConnection := &observationtest.DBConnectionMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows, mockConnection)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrUnrecognisedType)
				So(row, ShouldEqual, "")
			})
		})
	})
}

func TestBoltRowReader_BoltConnection_Closed(t *testing.T) {
	Convey("Given a row reader with a mock Bolt reader.", t, func() {
		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{666}, nil, nil
			},
		}

		mockConnection := &observationtest.DBConnectionMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows, mockConnection)

		Convey("When the row reader is closed the Bolt connection is released.", func() {
			err := rowReader.Close()
			So(err, ShouldBeNil)
			So(len(mockConnection.CloseCalls()), ShouldEqual, 1)
		})
	})
}
