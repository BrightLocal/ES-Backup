package item

import "encoding/json"

type Record struct {
	ID     string           `json:"id"`
	Type   string           `json:"type"`
	Source *json.RawMessage `json:"source"`
}
