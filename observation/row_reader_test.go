package observation_test

import (
	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/dp-filter/observation/observationtest"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"testing"
)

func TestBoltRowReader_Read(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader", t, func() {

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &observationtest.BoltRowsMock{
			CloseFunc: func() error {
				return nil
			},
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow}, nil, nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows)

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

		rowReader := observation.NewBoltRowReader(mockBoltRows)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The error from the Bolt reader is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, io.EOF)
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

		rowReader := observation.NewBoltRowReader(mockBoltRows)

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
				return []interface{}{666}, nil, nil
			},
		}

		rowReader := observation.NewBoltRowReader(mockBoltRows)

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
