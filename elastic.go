// Package elastic provides an Elasticsearch client with AWS sigv4 support.
package elastic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"text/template"

	"github.com/smartystreets/go-aws-auth"
)

// Credentials for AWS.
type Credentials awsauth.Credentials

// Client is an Elasticsearch client.
type Client struct {
	HTTPClient  *http.Client
	Credentials Credentials // Credentials for AWS role
	URL         string      // URL to Elasticsearch cluster
}

// New client.
func New(url string) *Client {
	return &Client{
		HTTPClient: http.DefaultClient,
		URL:        url,
	}
}

// Bulk POST request with the given body.
func (c *Client) Bulk(body io.Reader) error {
	return c.request("POST", "/_bulk", body, nil)
}

// DeleteIndex deletes `index`.
func (c *Client) DeleteIndex(index string) error {
	return c.request("DELETE", fmt.Sprintf("/%s", index), nil, nil)
}

// SearchIndex queries `index` and stores the results of `query` in `v`.
func (c *Client) SearchIndex(index string, query interface{}, v interface{}) error {
	b, err := json.Marshal(query)
	if err != nil {
		return err
	}

	return c.request("POST", fmt.Sprintf("/%s/_search", index), bytes.NewReader(b), v)
}

// SearchIndexString queries `index` and stores the results of `query` in `v`.
func (c *Client) SearchIndexString(index, query string, v interface{}) error {
	return c.request("POST", fmt.Sprintf("/%s/_search", index), strings.NewReader(query), v)
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
	return c.request("POST", "/_refresh", nil, nil)
}

// request performs a request against `url` storing the results as `v` when non-nil.
func (c *Client) request(method, path string, body io.Reader, v interface{}) error {
	req, err := http.NewRequest(method, c.URL+path, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	if c.Credentials.AccessKeyID != "" {
		req = awsauth.Sign4(req, awsauth.Credentials(c.Credentials))
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
