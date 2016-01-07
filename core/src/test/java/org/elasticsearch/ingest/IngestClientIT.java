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

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.delete.DeletePipelineAction;
import org.elasticsearch.action.ingest.delete.DeletePipelineRequestBuilder;
import org.elasticsearch.action.ingest.get.GetPipelineAction;
import org.elasticsearch.action.ingest.get.GetPipelineRequestBuilder;
import org.elasticsearch.action.ingest.get.GetPipelineResponse;
import org.elasticsearch.action.ingest.put.PutPipelineAction;
import org.elasticsearch.action.ingest.put.PutPipelineRequestBuilder;
import org.elasticsearch.action.ingest.simulate.SimulateDocumentSimpleResult;
import org.elasticsearch.action.ingest.simulate.SimulatePipelineAction;
import org.elasticsearch.action.ingest.simulate.SimulatePipelineRequestBuilder;
import org.elasticsearch.action.ingest.simulate.SimulatePipelineResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class IngestClientIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IngestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.ingest", true)
            .build();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
            .put(super.transportClientSettings())
            //TODO can we remove this?
             .put("node.ingest", true)
            .build();
    }

    public void testSimulate() throws Exception {
        new PutPipelineRequestBuilder(client(), PutPipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .field("description", "my_pipeline")
                        .startArray("processors")
                        .startObject()
                        .startObject("test")
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject().bytes())
                .get();
        GetPipelineResponse getResponse = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                .setIds("_id")
                .get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        SimulatePipelineResponse response = new SimulatePipelineRequestBuilder(client(), SimulatePipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .startArray("docs")
                        .startObject()
                        .field("_index", "index")
                        .field("_type", "type")
                        .field("_id", "id")
                        .startObject("_source")
                        .field("foo", "bar")
                        .field("fail", false)
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject().bytes())
                .get();

        assertThat(response.isVerbose(), equalTo(false));
        assertThat(response.getPipelineId(), equalTo("_id"));
        assertThat(response.getResults().size(), equalTo(1));
        assertThat(response.getResults().get(0), instanceOf(SimulateDocumentSimpleResult.class));
        SimulateDocumentSimpleResult simulateDocumentSimpleResult = (SimulateDocumentSimpleResult) response.getResults().get(0);
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        source.put("fail", false);
        source.put("processed", true);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, null, source);
        assertThat(simulateDocumentSimpleResult.getIngestDocument().getSourceAndMetadata(), equalTo(ingestDocument.getSourceAndMetadata()));
        assertThat(simulateDocumentSimpleResult.getFailure(), nullValue());
    }

    public void testBulkWithIngestFailures() throws Exception {
        createIndex("index");

        new PutPipelineRequestBuilder(client(), PutPipelineAction.INSTANCE)
            .setId("_id")
            .setSource(jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject().bytes())
            .get();

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id");
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index", "type", Integer.toString(i));
            indexRequest.source("field", "value", "fail", i % 2 == 0);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (i % 2 == 0) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                assertThat(failure.getMessage(), equalTo("java.lang.IllegalArgumentException: test processor failed"));
            } else {
                IndexResponse indexResponse = itemResponse.getResponse();
                assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
                assertThat(indexResponse.isCreated(), is(true));
            }
        }
    }

    public void test() throws Exception {
        new PutPipelineRequestBuilder(client(), PutPipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .field("description", "my_pipeline")
                        .startArray("processors")
                        .startObject()
                        .startObject("test")
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject().bytes())
                .get();
        GetPipelineResponse getResponse = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                .setIds("_id")
                .get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        client().prepareIndex("test", "type", "1").setSource("field", "value", "fail", false)
                .putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id")
                .get();

        Map<String, Object> doc = client().prepareGet("test", "type", "1")
                .get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value"));
        assertThat(doc.get("processed"), equalTo(true));

        client().prepareBulk().add(
                client().prepareIndex("test", "type", "2").setSource("field", "value2", "fail", false)
        ).putHeader(ConfigurationUtils.PIPELINE_ID_PARAM, "_id").get();
        doc = client().prepareGet("test", "type", "2").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value2"));
        assertThat(doc.get("processed"), equalTo(true));

        DeleteResponse response = new DeletePipelineRequestBuilder(client(), DeletePipelineAction.INSTANCE)
                .setId("_id")
                .get();
        assertThat(response.isFound(), is(true));
        assertThat(response.getId(), equalTo("_id"));

        getResponse = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                .setIds("_id")
                .get();
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Collections.emptyList();
    }

    public static class IngestPlugin extends Plugin {

        @Override
        public String name() {
            return "ingest";
        }

        @Override
        public String description() {
            return "ingest mock";
        }

        public void onModule(IngestModule ingestModule) {
            ingestModule.registerProcessor("test", (environment, templateService) -> config ->
                new TestProcessor("test", ingestDocument -> {
                    ingestDocument.setFieldValue("processed", true);
                    if (ingestDocument.getFieldValue("fail", Boolean.class)) {
                        throw new IllegalArgumentException("test processor failed");
                    }
                })
            );
        }
    }
}
