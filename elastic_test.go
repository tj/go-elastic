package elastic

import (
	"strings"
	"testing"

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

// Elastic endpoint.
var endpoint = "http://192.168.99.100:9200"

func newClient(t *testing.T) *Client {
	client := &Client{URL: endpoint}
	_ = client.DeleteIndex("pets")
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

	assert.NoError(t, client.RefreshIndex("pets"), "refreshing")

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

	assert.NoError(t, client.RefreshIndex("pets"), "refreshing")

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

	assert.NoError(t, client.RefreshIndex("pets"), "refreshing")

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
