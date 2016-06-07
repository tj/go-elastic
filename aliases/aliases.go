package aliases

import (
	"encoding/json"
	"time"
)

// Index aliases entry.
type Index struct {
	Aliases map[string]interface{}
}

// Indexes alias entries.
type Indexes map[string]Index

// Names returns the index names.
func (i Indexes) Names() (v []string) {
	for k := range i {
		v = append(v, k)
	}
	return
}

// Matching returns aliases matching the given time `layout`.
func (i Indexes) Matching(layout string) Indexes {
	out := make(Indexes)

	for k, v := range i {
		if _, err := time.Parse(layout, k); err != nil {
			continue
		}

		out[k] = v
	}

	return out
}

// MatchingOlderThan returns aliases matching the given `layout` which are older
// than `n` days relative to time `now`.
func (i Indexes) MatchingOlderThan(layout string, n int, now time.Time) Indexes {
	out := make(Indexes)
	old := now.AddDate(0, 0, -n)

	for k, v := range i {
		t, err := time.Parse(layout, k)
		if err != nil {
			continue
		}

		if !t.Before(old) {
			continue
		}

		out[k] = v
	}

	return out
}

// Action for index.
type Action struct {
	Remove struct {
		Index string `json:"index"`
		Alias string `json:"alias"`
	} `json:"remove"`
}

// Actions for indexes.
type Actions struct {
	Actions []Action `json:"actions"`
}

// RemoveOlderThan returns json for removing index from `alias` matching the given `layout`
// which are older than `n` days relative to `now`.
func (i Indexes) RemoveOlderThan(layout, alias string, n int, now time.Time) []byte {
	var actions Actions

	for index := range i.MatchingOlderThan(layout, n, now) {
		action := Action{}
		action.Remove.Alias = alias
		action.Remove.Index = index
		actions.Actions = append(actions.Actions, action)
	}

	if len(actions.Actions) == 0 {
		return nil
	}

	b, _ := json.Marshal(actions)
	return b
}
