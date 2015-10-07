/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class BasicTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(IngestPlugin.class);
    }

    public void test() throws Exception {
        client().prepareIndex(PipelineStore.INDEX, PipelineStore.TYPE, "_id")
                .setSource(jsonBuilder().startObject()
                        .field("name", "my_pipeline")
                        .field("description", "my_pipeline")
                        .startArray("processors")
                            .startObject()
                                .startObject("simple")
                                    .field("path", "field2")
                                    .field("expected_value", "abc")
                                    .field("add_field", "field3")
                                    .field("add_field_value", "xyz")
                                .endObject()
                            .endObject()
                        .endArray()
                        .endObject())
                .setRefresh(true)
                .get();
        Thread.sleep(5000);

        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field2", "abc")
                .putHeader("ingest", "_id")
                .get();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> doc = client().prepareGet("test", "type", "1")
                        .get().getSourceAsMap();
                assertThat(doc.get("field3"), equalTo("xyz"));
            }
        });

        client().prepareBulk().add(
                client().prepareIndex("test", "type", "2").setSource("field2", "abc")
        ).putHeader("ingest", "_id").get();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> doc = client().prepareGet("test", "type", "2").get().getSourceAsMap();
                assertThat(doc.get("field3"), equalTo("xyz"));
            }
        });
    }

    @Override
    protected boolean enableMockModules() {
        return false;
    }
}
