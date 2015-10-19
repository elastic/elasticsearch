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
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineAction;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineResponse;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineAction;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineResponse;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineAction;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

public class DatePipelineIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IngestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testBasics() throws Exception {
        new PutPipelineRequestBuilder(client(), PutPipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .field("description", "my_pipeline")
                        .startArray("processors")
                            .startObject()
                                .startObject("date")
                                    .field("timezone", "UTC")
                                    .field("locale", "en")
                                    .field("match_field", "my_date_field")
                                    .startArray("match_formats")
                                        .value("yyyy MMM dd HH:mm:ss Z")
                                     .endArray()
                                .endObject()
                            .endObject()
                        .endArray()
                        .endObject().bytes())
                .get();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetPipelineResponse response = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                        .setIds("_id")
                        .get();
                assertThat(response.isFound(), is(true));
                assertThat(response.pipelines().get("_id"), notNullValue());
            }
        });

        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("my_date_field", "2015 Nov 24 01:29:01 -0800")
                .putHeader("ingest", "_id")
                .get();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> doc = client().prepareGet("test", "type", "1")
                        .get().getSourceAsMap();
                assertThat(doc.get("@timestamp"), equalTo("2015-11-24T09:29:01.000Z"));
            }
        });
    }

    @Override
    protected boolean enableMockModules() {
        return false;
    }
}
