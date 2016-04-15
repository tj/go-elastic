package batch

import (
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/logfmt"
	"github.com/stretchr/testify/assert"

	"github.com/tj/go-elastic"
)

func init() {
	log.SetHandler(logfmt.Default)
}

// Elastic endpoint.
var endpoint = "http://192.168.99.101:9200"

func newClient(t *testing.T) *elastic.Client {
	client := &elastic.Client{URL: endpoint}
	_ = client.DeleteIndex("animals")
	return client
}

type pet struct {
	Name    string `json:"name"`
	Species string `json:"species"`
}

func TestClient_Bulk(t *testing.T) {
	client := newClient(t)

	batch := &Batch{
		Elastic: client,
		Index:   "animals",
		Type:    "pet",
		Log:     log.Log,
	}

	batch.Add(pet{"Tobi", "ferret"})
	batch.Add(pet{"Loki", "ferret"})
	batch.Add(pet{"Manny", "cat"})

	assert.Equal(t, 3, batch.Size(), "size")
	assert.NoError(t, batch.Flush(), "flush")
	assert.NoError(t, client.RefreshIndex("animals"), "refresh")

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

	assert.NoError(t, client.SearchIndexString("animals", query, &out))

	assert.Len(t, out.Aggregations.Species.Buckets, 2, "bucket length")
	assert.Equal(t, "ferret", out.Aggregations.Species.Buckets[0].Key)
	assert.Equal(t, 2, out.Aggregations.Species.Buckets[0].DocDount)
	assert.Equal(t, "cat", out.Aggregations.Species.Buckets[1].Key)
	assert.Equal(t, 1, out.Aggregations.Species.Buckets[1].DocDount)
}
