// Package elastic provides an Elasticsearch client with AWS sigv4 support.
package elastic

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/smartystreets/go-aws-auth"

	"github.com/tj/go-elastic/aliases"
)

// AWSCredentials for AWS.
type AWSCredentials awsauth.Credentials

// authCredentials to connect with a user/password combination
type authCredentials struct {
	username string
	password string
}

// BulkResponse for _bulk.
type BulkResponse struct {
	Took   float64             `json:"took"`
	Errors bool                `json:"errors"`
	Items  []*BulkResponseItem `json:"items"`
}

// BulkResponseItem for _bulk.
type BulkResponseItem struct {
	Create *BulkResponseItemResult `json:"create,omitempty"`
	Delete *BulkResponseItemResult `json:"delete,omitempty"`
	Update *BulkResponseItemResult `json:"update,omitempty"`
	Index  *BulkResponseItemResult `json:"index,omitempty"`
}

// BulkResponseItem for _bulk request responses.
type BulkResponseItemResult struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Status  int    `json:"status"`
	Found   bool   `json:"bool,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Client is an Elasticsearch client.
type Client struct {
	HTTPClient      *http.Client
	awsCredentials  *AWSCredentials  // Credentials for AWS role
	authCredentials *authCredentials // User/password credentials
	URL             string           // URL to Elasticsearch cluster
}

// New client.
func New(url string) *Client {
	return &Client{
		HTTPClient: http.DefaultClient,
		URL:        url,
	}
}

// SetAWSCredentials for connection to an AWS ElasticSearch instance
func (c *Client) SetAWSCredentials(credentials AWSCredentials) {
	c.awsCredentials = &credentials
	c.authCredentials = nil
}

// SetAuthCredentials for a username/password connection
func (c *Client) SetAuthCredentials(username, password string) {
	c.authCredentials = &authCredentials{
		username: username,
		password: password,
	}
	c.awsCredentials = nil
}

// Bulk POST request with the given body.
func (c *Client) Bulk(body io.Reader) error {
	return c.Request("POST", "/_bulk", body, nil)
}

// BulkResponse POST request with the given body and return response.
func (c *Client) BulkResponse(body io.Reader) (res *BulkResponse, err error) {
	res = new(BulkResponse)
	err = c.Request("POST", "/_bulk", body, res)
	return
}

// DeleteIndex deletes `index`.
func (c *Client) DeleteIndex(index string) error {
	return c.Request("DELETE", fmt.Sprintf("/%s", index), nil, nil)
}

// DeleteAll deletes all indexes.
func (c *Client) DeleteAll() error {
	return c.Request("DELETE", "/_all", nil, nil)
}

// Aliases returns indexes and their aliases.
func (c *Client) Aliases() (v aliases.Indexes, err error) {
	err = c.Request("GET", "/_aliases", nil, &v)
	return
}

// RemoveOldAliases removes `alias` from timeseries style indexes older than `n` days based on `layout`
// such as "logs-06-01-02". For example to maintain the past week (inclusive) you might use
// RemoveOldAliases("logs-06-01-02", "last_week", 8, time.Now()).
func (c *Client) RemoveOldAliases(layout, alias string, n int, now time.Time) error {
	indexes, err := c.Aliases()
	if err != nil {
		return err
	}

	body := indexes.RemoveOlderThan(layout, alias, n, now)
	if body == nil {
		return nil
	}

	return c.Request("POST", "/_aliases", bytes.NewReader(body), nil)
}

// RemoveOldIndexes removes indexes from timeseries style indexes older than `n` days based on `layout`
// such as "logs-06-01-02". For example to maintain the past week (inclusive) you might use
// RemoveOldIndexes("logs-06-01-02", 8, time.Now()).
func (c *Client) RemoveOldIndexes(layout string, n int, now time.Time) error {
	indexes, err := c.Aliases()
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return nil
	}

	names := indexes.MatchingOlderThan(layout, n, now).Names()
	if len(names) == 0 {
		return nil
	}

	return c.DeleteIndex(strings.Join(names, ","))
}

// SearchIndex queries `index` and stores the results of `query` in `v`.
func (c *Client) SearchIndex(index string, query interface{}, v interface{}) error {
	b, err := json.Marshal(query)
	if err != nil {
		return err
	}

	return c.Request("POST", fmt.Sprintf("/%s/_search", index), bytes.NewReader(b), v)
}

// SearchIndexString queries `index` and stores the results of `query` in `v`.
func (c *Client) SearchIndexString(index, query string, v interface{}) error {
	return c.Request("POST", fmt.Sprintf("/%s/_search", index), strings.NewReader(query), v)
}

// SearchIndexTemplate queries `index` with `tmpl` string and stores the results in `v`.
func (c *Client) SearchIndexTemplate(index, tmpl string, data interface{}, v interface{}) error {
	var buf bytes.Buffer

	t, err := template.New("main").Parse(tmpl)
	if err != nil {
		return err
	}

	if err := t.Execute(&buf, data); err != nil {
		return err
	}

	return c.SearchIndexString(index, buf.String(), v)
}

// RefreshIndex refreshes `index`.
func (c *Client) RefreshIndex(index string) error {
	return c.Request("POST", fmt.Sprintf("/%s/_refresh", index), nil, nil)
}

// RefreshAll refreshes all indexes.
func (c *Client) RefreshAll() error {
	return c.Request("POST", "/_refresh", nil, nil)
}

// Request performs a request against `url` storing the results as `v` when non-nil.
func (c *Client) Request(method, path string, body io.Reader, v interface{}) error {
	req, err := http.NewRequest(method, c.URL+path, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	if c.authCredentials != nil {
		credentials := fmt.Sprintf("%s:%s", c.authCredentials.username, c.authCredentials.password)
		b64credentials := base64.StdEncoding.EncodeToString([]byte(credentials))
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", b64credentials))
	} else if c.awsCredentials != nil {
		req = awsauth.Sign4(req, awsauth.Credentials(*c.awsCredentials))
		if req == nil {
			return errors.New("elastic: error signing request")
		}
	}

	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", res.Status, b)
	}

	if v != nil {
		return json.Unmarshal(b, v)
	}

	return nil
}
