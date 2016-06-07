package batch

import (
	"os"
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
var endpoint = os.Getenv("ES_ADDR")

func newClient(t *testing.T) *elastic.Client {
	client := elastic.New(endpoint)
	assert.NoError(t, client.DeleteAll(), "deleting all")
	assert.NoError(t, client.RefreshAll(), "refreshing")
	return client
}

type pet struct {
	Name    string `json:"name"`
	Species string `json:"species"`
}

func TestClient_Bulk(t *testing.T) {
	t.SkipNow() // TODO: refreshing seems to be disregarded after _bulk?

	client := newClient(t)

	batch := &Batch{
		Elastic: client,
		Index:   "animals",
		Type:    "pet",
	}

	batch.Add(pet{"Tobi", "ferret"})
	batch.Add(pet{"Loki", "ferret"})
	batch.Add(pet{"Manny", "cat"})

	assert.Equal(t, 3, batch.Size(), "size")
	assert.NoError(t, batch.Flush(), "flush")
	assert.Equal(t, 0, batch.Size(), "size")
	assert.NoError(t, client.RefreshAll(), "refresh")

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
