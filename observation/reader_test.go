package observation_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/dp-filter/observation/observationtest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReader_Read(t *testing.T) {

	Convey("Given a reader with a mock CSV row reader that returns a single row", t, func() {

		dummyRowContent := "csv,row,content"

		mockRowReader := &observationtest.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				return dummyRowContent, nil
			},
		}

		reader := observation.NewReader(mockRowReader)

		expected := []byte(dummyRowContent)

		Convey("When read is called", func() {

			var actual []byte = make([]byte, len(expected))

			bytesRead, err := reader.Read(actual)

			Convey("The mock row content is returned", func() {
				So(err, ShouldBeNil)
				So(bytesRead, ShouldEqual, len(expected))
				So(reader.TotalBytesRead(), ShouldEqual, len(expected))

				So(reflect.DeepEqual(expected, actual), ShouldBeTrue)
			})
		})
	})
}

func TestReader_Read_MultipleReadsForALine(t *testing.T) {

	Convey("Given a reader with a mock CSV row reader that returns a single row", t, func() {

		dummyRowContent := "csv,row,content" // 15 bytes total
		bufferLen := 6

		mockRowReader := &observationtest.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				return dummyRowContent, io.EOF
			},
		}

		reader := observation.NewReader(mockRowReader)

		Convey("When read is called multiple times with a buffer that is smaller than the line", func() {

			var read1 []byte = make([]byte, bufferLen)
			var read2 []byte = make([]byte, bufferLen)
			var read3 []byte = make([]byte, bufferLen)

			expected1 := []byte(dummyRowContent)[:bufferLen]
			expected2 := []byte(dummyRowContent)[bufferLen : bufferLen*2]
			expected3 := []byte(dummyRowContent)[bufferLen*2:]

			bytesRead1, err1 := reader.Read(read1)
			bytesRead2, err2 := reader.Read(read2)
			bytesRead3, err3 := reader.Read(read3)

			Convey("The expected content is in the buffers, and the last call gives io.EOF", func() {

				So(err1, ShouldBeNil)
				So(bytesRead1, ShouldEqual, bufferLen)
				So(reflect.DeepEqual(expected1, read1), ShouldBeTrue)

				So(err2, ShouldBeNil)
				So(bytesRead2, ShouldEqual, bufferLen)
				So(reflect.DeepEqual(expected2, read2), ShouldBeTrue)

				So(err3, ShouldNotBeNil)
				So(bytesRead3, ShouldEqual, 3)
				So(reflect.DeepEqual(expected3[:3], read3[:3]), ShouldBeTrue)

				So(reader.TotalBytesRead(), ShouldEqual, len([]byte(dummyRowContent)))
			})
		})
	})
}

func TestReader_Read_MultipleLines(t *testing.T) {

	Convey("Given a reader with a mock CSV row reader that returns a single row", t, func() {

		dummyRowContent := "csv,row,content" // 15 bytes total
		bufferLen := 20

		mockRowReader := &observationtest.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				return dummyRowContent, nil
			},
		}

		reader := observation.NewReader(mockRowReader)

		Convey("When read is called using a buffer that is larger than the line", func() {

			var read1 []byte = make([]byte, bufferLen)
			var read2 []byte = make([]byte, bufferLen)
			var read3 []byte = make([]byte, bufferLen)

			expected := []byte(dummyRowContent)
			expectedLen := len(expected)

			bytesRead1, err1 := reader.Read(read1)
			bytesRead2, err2 := reader.Read(read2)
			bytesRead3, err3 := reader.Read(read3)

			Convey("The expected content is in the buffers, and the last call gives io.EOF", func() {

				So(err1, ShouldBeNil)
				So(bytesRead1, ShouldEqual, expectedLen)
				So(reflect.DeepEqual(expected, read1[:expectedLen]), ShouldBeTrue)

				So(err2, ShouldBeNil)
				So(bytesRead2, ShouldEqual, expectedLen)
				So(reflect.DeepEqual(expected, read2[:expectedLen]), ShouldBeTrue)

				So(err3, ShouldBeNil)
				So(bytesRead3, ShouldEqual, expectedLen)
				So(reflect.DeepEqual(expected, read3[:expectedLen]), ShouldBeTrue)

				So(reader.TotalBytesRead(), ShouldEqual, expectedLen*3) // should have read the row content 3 times
				So(reader.ObservationsCount(), ShouldEqual, 3)
			})
		})
	})
}

func TestReader_Read_Error(t *testing.T) {

	Convey("Given a reader with a mock CSV row reader that returns an error", t, func() {

		expectedError := io.EOF

		mockRowReader := &observationtest.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				return "", expectedError
			},
		}

		reader := observation.NewReader(mockRowReader)

		Convey("When read is called", func() {

			var actual []byte = make([]byte, 0)

			bytesRead, err := reader.Read(actual)

			Convey("The error from the CSV row reader is returned", func() {
				So(err, ShouldNotBeNil)
				So(bytesRead, ShouldEqual, 0)
				So(err, ShouldEqual, expectedError)
				So(reader.TotalBytesRead(), ShouldEqual, 0)
			})
		})
	})
}
