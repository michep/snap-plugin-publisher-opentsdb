/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package opentsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	putEndPoint     = "/api/put"
	contentTypeJSON = "application/json"
)

type HttpClient struct {
	url        url.URL
	httpClient *http.Client
	userAgent  string
	chunksize  int
}

type Client interface {
	NewClient(url string, timeout time.Duration) *HttpClient
}

//NewClient creates an instance of HttpClient which times out at
//the givin duration.
func NewClient(url url.URL, chunksize int, timeout int) *HttpClient {
	return &HttpClient{
		url: url,
		httpClient: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
		chunksize: chunksize,
	}
}

func (hc *HttpClient) getURL() string {
	u := hc.url
	u.Path = putEndPoint
	return u.String()
}

// Save saves data points in maxChunkLength size.
func (hc *HttpClient) Save(dps []DataPoint) error {
	u := hc.getURL()

	loop := len(dps) / hc.chunksize
	start := 0
	end := start
	for i := 0; i < loop; i++ {
		end += hc.chunksize
		chunk := dps[start:end]
		start = end
		err := hc.post(u, chunk)
		if err != nil {
			return err
		}
	}

	remainder := len(dps) % hc.chunksize
	if remainder > 0 {
		end = start + remainder
		chunk := dps[start:end]
		return hc.post(u, chunk)
	}
	return nil
}

// post stores a slice of Datapoint to OpenTSDB
func (hc *HttpClient) post(url string, dps []DataPoint) error {
	buf, err := json.Marshal(dps)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", contentTypeJSON)

	resp, err := hc.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	default:
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var details, msg string
		var result map[string]interface{}
		if json.Unmarshal(content, &result) == nil {
			details = fmt.Sprintf("Details: %v", result["error"].(map[string]interface{})["details"])

			msg = fmt.Sprintf("Code: %v, message: %v",
				result["error"].(map[string]interface{})["code"],
				result["error"].(map[string]interface{})["message"])
		} else {
			details = fmt.Sprintf("Details: %s", string(content))
			msg = ""
		}

		fmt.Fprintf(os.Stderr, "Failed to post data to OpenTSDB: %s", details)
		return fmt.Errorf("failed to post data to OpenTSDB: %v. For more information check stderr file", msg)
	}
}
