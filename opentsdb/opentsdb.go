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
	"fmt"
	"net/url"
	"strings"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	log "github.com/sirupsen/logrus"
)

const (
	Name               = "opentsdb"
	Version            = 10
	hostTag            = "host"
	pluginrunningonTag = "plugin_running_on"
)

//NewOpentsdbPublisher returns an instance of the OpenTSDB publisher
func NewOpentsdbPublisher() *opentsdbPublisher {
	return &opentsdbPublisher{}
}

type opentsdbPublisher struct {
	client *HttpClient
}

func (p *opentsdbPublisher) GetConfigPolicy() (plugin.ConfigPolicy, error) {
	cp := plugin.NewConfigPolicy()
	cp.AddNewStringRule([]string{""}, "host", true)
	cp.AddNewIntRule([]string{""}, "port", true)
	cp.AddNewIntRule([]string{""}, "chunksize", false, plugin.SetDefaultInt(25))
	cp.AddNewIntRule([]string{""}, "timeout", false, plugin.SetDefaultInt(5))
	return *cp, nil
}

// Publish publishes metric data to opentsdb.
func (p *opentsdbPublisher) Publish(mts []plugin.Metric, config plugin.Config) error {
	logger := log.New()
	if p.client == nil {
		host, err := config.GetString("host")
		if err != nil {
			handleErr(err)
		}
		port, err := config.GetInt("port")
		if err != nil {
			handleErr(err)
		}
		chunksize, err := config.GetInt("chunksize")
		if err != nil {
			handleErr(err)
		}
		timeout, err := config.GetInt("timeout")
		if err != nil {
			handleErr(err)
		}
		u, err := url.Parse(fmt.Sprintf("%s:%d", host, port))
		if err != nil {
			handleErr(err)
		}

		p.client = NewClient(*u, int(chunksize), int(timeout))
	}

	var pts []DataPoint
	var temp DataPoint
	for _, m := range mts {
		tempTags := make(map[string]StringValue)
		isDynamic, indexes := m.Namespace.IsDynamic()
		ns := m.Namespace.Strings()
		if isDynamic {
			for i, j := range indexes {
				// The second return value from IsDynamic(), in this case `indexes`, is the index of
				// the dynamic element in the unmodified namespace. However, here we're deleting
				// elements, which is problematic when the number of dynamic elements in a namespace is
				// greater than 1. Therefore, we subtract i (the loop iteration) from j
				// (the original index) to compensate.
				//
				// Remove "data" from the namespace and create a tag for it
				ns = append(ns[:j-i], ns[j-i+1:]...)
				tempTags[m.Namespace[j].Name] = StringValue(m.Namespace[j].Value)
			}
		}

		for k, v := range m.Tags {
			tempTags[k] = StringValue(v)
		}

		tempTags[hostTag] = StringValue(m.Tags[pluginrunningonTag])

		temp = DataPoint{
			Metric:    StringValue(strings.Join(ns, ".")),
			Timestamp: m.Timestamp.Unix(),
			Value:     m.Data,
			Tags:      tempTags,
		}

		// Omits invalid data points
		if temp.Valid() {
			pts = append(pts, temp)
		} else {
			logger.Printf("Omitted invalid data point %s (non-numeric values not allowed in OpenTSDB)", temp.Metric)
		}
	}

	if len(pts) == 0 {
		logger.Printf("Info: '%s' posting metrics: %+v", "no valid data", mts)
		return nil
	}

	err := p.client.Save(pts)
	if err != nil {
		logger.Printf("Error: '%s' posting metrics: %+v", err.Error(), mts)
		return err
	}

	return nil
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
