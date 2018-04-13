package observation

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var filter = Filter{
	InstanceID:       "1234567890",
	FilterID:         "0987654321",
	DimensionFilters: nil,
	Published:        &Published,
}

func TestFilter_IsEmpty(t *testing.T) {
	Convey("Given dimensionFilters is nil", t, func() {
		filter.DimensionFilters = nil

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters is empty", t, func() {
		filter.DimensionFilters = []*DimensionFilter{}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters contains only empty values", t, func() {
		filter.DimensionFilters = []*DimensionFilter{
			&DimensionFilter{
				Options: []string{""},
				Name:    "",
			},
		}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters contains non empty values", t, func() {
		filter.DimensionFilters = []*DimensionFilter{
			&DimensionFilter{
				Options: []string{"JAN"},
				Name:    "Time",
			},
		}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeFalse)
		})
	})
}
