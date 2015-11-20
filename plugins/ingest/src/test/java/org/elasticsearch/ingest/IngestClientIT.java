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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineAction;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineResponse;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineAction;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineResponse;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineAction;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulateDocumentSimpleResult;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineAction;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineRequestBuilder;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class IngestClientIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IngestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testSimulate() throws Exception {
        new PutPipelineRequestBuilder(client(), PutPipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .field("description", "my_pipeline")
                        .startArray("processors")
                        .startObject()
                        .startObject("grok")
                        .field("field", "field1")
                        .field("pattern", "%{NUMBER:val:float} %{NUMBER:status:int} <%{WORD:msg}>")
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
        IngestDocument expectedIngestDocument = new IngestDocument("index", "type", "id", Collections.singletonMap("foo", "bar"));
        assertThat(simulateDocumentSimpleResult.getData(), equalTo(expectedIngestDocument));
        assertThat(simulateDocumentSimpleResult.getFailure(), nullValue());
    }

    public void testBulkWithIngestFailures() {
        createIndex("index");

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_none_existing_id");
        for (int i = 0; i < numRequests; i++) {
            if (i % 2 == 0) {
                UpdateRequest updateRequest = new UpdateRequest("index", "type", Integer.toString(i));
                updateRequest.upsert("field", "value");
                updateRequest.doc(new HashMap());
                bulkRequest.add(updateRequest);
            } else {
                IndexRequest indexRequest = new IndexRequest("index", "type", Integer.toString(i));
                indexRequest.source("field1", "value1");
                bulkRequest.add(indexRequest);
            }
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            ActionRequest request = bulkRequest.requests().get(i);
            BulkItemResponse itemResponse = response.getItems()[i];
            if (request instanceof IndexRequest) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                assertThat(failure.getMessage(), equalTo("java.lang.IllegalArgumentException: pipeline with id [_none_existing_id] does not exist"));
            } else if (request instanceof UpdateRequest) {
                UpdateResponse updateResponse = itemResponse.getResponse();
                assertThat(updateResponse.getId(), equalTo(Integer.toString(i)));
                assertThat(updateResponse.isCreated(), is(true));
            } else {
                fail("unexpected request item [" + request + "]");
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
                        .startObject("grok")
                        .field("field", "field1")
                        .field("pattern", "%{NUMBER:val:float} %{NUMBER:status:int} <%{WORD:msg}>")
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
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("properties")
                .startObject("status").field("type", "integer").endObject()
                .startObject("val").field("type", "float").endObject()
                .endObject();
        PutMappingResponse putMappingResponse = client().admin().indices()
                .preparePutMapping("test").setType("type").setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        client().prepareIndex("test", "type", "1").setSource("field1", "123.42 400 <foo>")
                .putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id")
                .get();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> doc = client().prepareGet("test", "type", "1")
                        .get().getSourceAsMap();
                assertThat(doc.get("val"), equalTo(123.42));
                assertThat(doc.get("status"), equalTo(400));
                assertThat(doc.get("msg"), equalTo("foo"));
            }
        });

        client().prepareBulk().add(
                client().prepareIndex("test", "type", "2").setSource("field1", "123.42 400 <foo>")
        ).putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id").get();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> doc = client().prepareGet("test", "type", "2").get().getSourceAsMap();
                assertThat(doc.get("val"), equalTo(123.42));
                assertThat(doc.get("status"), equalTo(400));
                assertThat(doc.get("msg"), equalTo("foo"));
            }
        });

        DeletePipelineResponse response = new DeletePipelineRequestBuilder(client(), DeletePipelineAction.INSTANCE)
                .setId("_id")
                .get();
        assertThat(response.found(), is(true));
        assertThat(response.id(), equalTo("_id"));

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetPipelineResponse response = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                        .setIds("_id")
                        .get();
                assertThat(response.isFound(), is(false));
                assertThat(response.pipelines().get("_id"), nullValue());
            }
        });
    }

    @Override
    protected boolean enableMockModules() {
        return false;
    }
}
