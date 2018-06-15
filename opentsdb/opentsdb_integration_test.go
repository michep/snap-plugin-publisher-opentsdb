// +build medium

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
	"os"
	"testing"
	"time"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	. "github.com/smartystreets/goconvey/convey"
)

func TestOpentsdbPublish(t *testing.T) {
	config := plugin.NewConfig()

	Convey("Snap Plugin integration testing with OpenTSDB", t, func() {
		config["host"] = os.Getenv("SNAP_OPENTSDB_HOST")
		config["port"] = 4242

		op := NewOpentsdbPublisher()
		tags := map[string]string{}
		tags[pluginrunningonTag] = "mac1"

		Convey("Publish float metrics to OpenTSDB", func() {
			metrics := []plugin.Metric{
				{Namespace: plugin.NewNamespace("/psutil/load/load15"), Timestamp: time.Now(), Tags: tags, Data: 23.1},
				{Namespace: plugin.NewNamespace("/psutil/vm/available"), Timestamp: time.Now().Add(2 * time.Second), Tags: tags, Data: 23.2},
				{Namespace: plugin.NewNamespace("/psutil/load/load1"), Timestamp: time.Now().Add(3 * time.Second), Tags: tags, Data: 23.3},
			}
			err := op.Publish(metrics, config)
			So(err, ShouldBeNil)
		})

		Convey("Publish int metrics to OpenTSDB", func() {
			metrics := []plugin.Metric{
				{Namespace: plugin.NewNamespace("/psutil/vm/free"), Timestamp: time.Now().Add(5 * time.Second), Tags: tags, Data: 23},
			}
			err := op.Publish(metrics, config)
			So(err, ShouldBeNil)
		})
	})
}
