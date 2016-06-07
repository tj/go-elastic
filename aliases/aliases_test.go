package aliases

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var aliases = []byte(`{
  ".kibana-4" : {
    "aliases" : { }
  },
  "logs-16-04-01" : {
    "aliases" : {
      "logs" : { }
    }
  },
  "logs-16-04-02" : {
    "aliases" : {
      "logs" : { }
    }
  },
  "checks-16-04-01" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-02" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-03" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-04" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-05" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-06" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-07" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-08" : {
    "aliases" : {
      "checks" : { }
    }
  },
  "checks-16-04-09" : {
    "aliases" : {
      "checks" : { }
    }
  }
}`)

func genIndexes(t *testing.T) Indexes {
	var indexes Indexes
	assert.NoError(t, json.Unmarshal(aliases, &indexes))
	return indexes
}

func keys(m Indexes) (keys []string) {
	for k, _ := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return
}

func TestIndexes_Matching(t *testing.T) {
	indexes := genIndexes(t)
	assert.Len(t, indexes.Matching("checks-06-01-02"), 9)
	assert.Len(t, indexes.Matching("logs-06-01-02"), 2)
	assert.Len(t, indexes.Matching("something"), 0)
}

func TestIndexes_MatchingOlderThan(t *testing.T) {
	indexes := genIndexes(t)
	now, err := time.Parse("2006-01-02", "2016-04-09")
	assert.NoError(t, err, "error parsing time")
	out := indexes.MatchingOlderThan("checks-06-01-02", 7, now.Add(time.Hour))
	assert.Equal(t, []string{"checks-16-04-01", "checks-16-04-02"}, keys(out))
}

func TestIndexes_MatchingOlderThan_day(t *testing.T) {
	indexes := genIndexes(t)
	now, err := time.Parse("2006-01-02", "2016-04-09")
	assert.NoError(t, err, "error parsing time")
	out := indexes.MatchingOlderThan("checks-06-01-02", 1, now.Add(time.Hour))
	assert.Equal(t, []string{"checks-16-04-01", "checks-16-04-02", "checks-16-04-03", "checks-16-04-04", "checks-16-04-05", "checks-16-04-06", "checks-16-04-07", "checks-16-04-08"}, keys(out))
}

func TestIndexes_MatchingOlderThan_day2(t *testing.T) {
	indexes := genIndexes(t)
	now, err := time.Parse("2006-01-02", "2016-04-09")
	assert.NoError(t, err, "error parsing time")
	out := indexes.MatchingOlderThan("checks-06-01-02", 2, now.Add(time.Hour))
	assert.Equal(t, []string{"checks-16-04-01", "checks-16-04-02", "checks-16-04-03", "checks-16-04-04", "checks-16-04-05", "checks-16-04-06", "checks-16-04-07"}, keys(out))
}

func TestIndexes_RemoveOlderThan(t *testing.T) {
	indexes := genIndexes(t)
	now, err := time.Parse("2006-01-02", "2016-04-09")
	assert.NoError(t, err, "error parsing time")
	out := indexes.RemoveOlderThan("checks-06-01-02", "checks", 7, now.Add(time.Minute))
	assert.Equal(t, `{"actions":[{"remove":{"index":"checks-16-04-01","alias":"checks"}},{"remove":{"index":"checks-16-04-02","alias":"checks"}}]}`, string(out))
}
