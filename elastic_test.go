package elastic

import (
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var docs = `{ "index": { "_index": "pets", "_type": "pet" }}
{ "name": "Tobi", "species": "ferret" }
{ "index": { "_index": "pets", "_type": "pet" }}
{ "name": "Loki", "species": "ferret" }
{ "index": { "_index": "pets", "_type": "pet" }}
{ "name": "Jane", "species": "ferret" }
{ "index": { "_index": "pets", "_type": "pet" }}
{ "name": "Manny", "species": "cat" }
{ "index": { "_index": "pets", "_type": "pet" }}
{ "name": "Luna", "species": "cat" }
`

var seriesDocs = `{ "index": { "_index": "series-16-01-20", "_type": "series" }}
{ "n": 1 }
{ "index": { "_index": "series-16-01-21", "_type": "series" }}
{ "n": 2 }
{ "index": { "_index": "series-16-01-21", "_type": "series" }}
{ "n": 3 }
{ "index": { "_index": "series-16-01-22", "_type": "series" }}
{ "n": 4 }
{ "index": { "_index": "series-16-01-23", "_type": "series" }}
{ "n": 5 }
`

// Elastic endpoint.
var endpoint = os.Getenv("ES_ADDR")

func newClient(t *testing.T) *Client {
	client := New(endpoint)
	_ = client.DeleteAll()
	assert.NoError(t, client.RefreshAll(), "refreshing")
	return client
}

func TestClient_Bulk(t *testing.T) {
	client := newClient(t)
	assert.NoError(t, client.Bulk(strings.NewReader(docs)))
}

func TestClient_Bulk_error(t *testing.T) {
	client := newClient(t)
	err := client.Bulk(strings.NewReader(""))
	assert.Error(t, err, `No error is expected but got 400 Bad Request: {"error":{"root_cause":[{"type":"parse_exception","reason":"Failed to derive xcontent"}],"type":"parse_exception","reason":"Failed to derive xcontent"},"status":400}`)
}

func TestClient_SearchIndexString(t *testing.T) {
	client := newClient(t)
	assert.NoError(t, client.Bulk(strings.NewReader(docs)))

	assert.NoError(t, client.RefreshAll(), "refreshing")

	query := `{
    "aggs": {
      "species": {
        "terms": {
          "field": "species"
        }
      }
    }
  }`

	var out struct {
		Aggregations struct {
			Species struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocDount int    `json:"doc_count"`
				}
			}
		}
	}

	assert.NoError(t, client.SearchIndexString("pets", query, &out))

	assert.Len(t, out.Aggregations.Species.Buckets, 2, "bucket length")
	assert.Equal(t, "ferret", out.Aggregations.Species.Buckets[0].Key)
	assert.Equal(t, 3, out.Aggregations.Species.Buckets[0].DocDount)
	assert.Equal(t, "cat", out.Aggregations.Species.Buckets[1].Key)
	assert.Equal(t, 2, out.Aggregations.Species.Buckets[1].DocDount)
}

func TestClient_SearchIndex(t *testing.T) {
	client := newClient(t)
	assert.NoError(t, client.Bulk(strings.NewReader(docs)))

	assert.NoError(t, client.RefreshAll(), "refreshing")

	var query struct {
		Aggs struct {
			Species struct {
				Terms struct {
					Field string `json:"field"`
				} `json:"terms"`
			} `json:"species"`
		} `json:"aggs"`
	}

	query.Aggs.Species.Terms.Field = "species"

	var out struct {
		Aggregations struct {
			Species struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocDount int    `json:"doc_count"`
				}
			}
		}
	}

	assert.NoError(t, client.SearchIndex("pets", query, &out))

	assert.Len(t, out.Aggregations.Species.Buckets, 2, "bucket length")
	assert.Equal(t, "ferret", out.Aggregations.Species.Buckets[0].Key)
	assert.Equal(t, 3, out.Aggregations.Species.Buckets[0].DocDount)
	assert.Equal(t, "cat", out.Aggregations.Species.Buckets[1].Key)
	assert.Equal(t, 2, out.Aggregations.Species.Buckets[1].DocDount)
}

func TestClient_SearchIndexTemplate(t *testing.T) {
	client := newClient(t)
	assert.NoError(t, client.Bulk(strings.NewReader(docs)))

	assert.NoError(t, client.RefreshAll(), "refreshing")

	query := `{
    "aggs": {
      "species": {
        "terms": {
          "field": "{{.Field}}"
        }
      }
    }
  }`

	var in = struct {
		Field string
	}{"species"}

	var out struct {
		Aggregations struct {
			Species struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocDount int    `json:"doc_count"`
				}
			}
		}
	}

	assert.NoError(t, client.SearchIndexTemplate("pets", query, in, &out))

	assert.Len(t, out.Aggregations.Species.Buckets, 2, "bucket length")
	assert.Equal(t, "ferret", out.Aggregations.Species.Buckets[0].Key)
	assert.Equal(t, 3, out.Aggregations.Species.Buckets[0].DocDount)
	assert.Equal(t, "cat", out.Aggregations.Species.Buckets[1].Key)
	assert.Equal(t, 2, out.Aggregations.Species.Buckets[1].DocDount)
}

func TestClient_Aliases(t *testing.T) {
	client := newClient(t)

	indexes, err := client.Aliases()
	assert.NoError(t, err, "error fetching aliases")
	assert.Empty(t, indexes, "aliases should be empty")

	assert.NoError(t, client.Bulk(strings.NewReader(seriesDocs)))
	assert.NoError(t, client.RefreshAll(), "refreshing")

	indexes, err = client.Aliases()
	assert.NoError(t, err, "error fetching aliases")

	names := indexes.Names()
	sort.Strings(names)
	assert.Equal(t, []string{"series-16-01-20", "series-16-01-21", "series-16-01-22", "series-16-01-23"}, names)
}

func TestClient_RemoveOldIndexes(t *testing.T) {
	client := newClient(t)

	assert.NoError(t, client.Bulk(strings.NewReader(seriesDocs)))
	assert.NoError(t, client.RefreshAll(), "refreshing")

	now, err := time.Parse("2006-01-02", "2016-01-23")
	assert.NoError(t, err, "error parsing time")

	assert.NoError(t, client.RemoveOldIndexes("series-06-01-02", 2, now.Add(time.Minute)), "removing")
	assert.NoError(t, client.RefreshAll(), "refreshing")

	indexes, err := client.Aliases()
	assert.NoError(t, err, "error fetching aliases")

	names := indexes.Names()
	sort.Strings(names)
	assert.Equal(t, []string{"series-16-01-22", "series-16-01-23"}, names)
}
