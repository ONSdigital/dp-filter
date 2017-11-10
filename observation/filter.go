package observation

// Filter represents a structure for a filter job
type Filter struct {
	FilterID         string             `json:"filter_id,omitempty"`
	InstanceID       string             `json:"instance_id"`
	DimensionFilters []*DimensionFilter `json:"dimensions,omitempty"`
}

// DimensionFilter represents an object containing a list of dimension values and the dimension name
type DimensionFilter struct {
	Name    string   `json:"name,omitempty"`
	Options []string `json:"options,omitempty"`
}
