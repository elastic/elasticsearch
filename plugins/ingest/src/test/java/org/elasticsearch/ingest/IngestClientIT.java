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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineAction;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineRequestBuilder;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
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
        assertThat(simulateDocumentSimpleResult.getIngestDocument(), nullValue());
        assertThat(simulateDocumentSimpleResult.getFailure(), notNullValue());

        response = new SimulatePipelineRequestBuilder(client(), SimulatePipelineAction.INSTANCE)
                .setId("_id")
                .setSource(jsonBuilder().startObject()
                        .startArray("docs")
                        .startObject()
                        .field("_index", "index")
                        .field("_type", "type")
                        .field("_id", "id")
                        .startObject("_source")
                        .field("field1", "123.42 400 <foo>")
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject().bytes())
                .get();

        assertThat(response.isVerbose(), equalTo(false));
        assertThat(response.getPipelineId(), equalTo("_id"));
        assertThat(response.getResults().size(), equalTo(1));
        assertThat(response.getResults().get(0), instanceOf(SimulateDocumentSimpleResult.class));
        simulateDocumentSimpleResult = (SimulateDocumentSimpleResult) response.getResults().get(0);
        Map<String, Object> source = new HashMap<>();
        source.put("field1", "123.42 400 <foo>");
        source.put("val", 123.42f);
        source.put("status", 400);
        source.put("msg", "foo");
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
                .startObject("join")
                .field("field", "field1")
                .field("separator", "|")
                .endObject()
                .endObject()
                .endArray()
                .endObject().bytes())
            .get();

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id");
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index", "type", Integer.toString(i));
            if (i % 2 == 0) {
                indexRequest.source("field1", Arrays.asList("value1", "value2"));
            } else {
                indexRequest.source("field2", Arrays.asList("value1", "value2"));
            }
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (i % 2 == 0) {
                IndexResponse indexResponse = itemResponse.getResponse();
                assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
                assertThat(indexResponse.isCreated(), is(true));
            } else {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                assertThat(failure.getMessage(), equalTo("java.lang.IllegalArgumentException: field [field1] not present as part of path [field1]"));
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
        GetPipelineResponse getResponse = new GetPipelineRequestBuilder(client(), GetPipelineAction.INSTANCE)
                .setIds("_id")
                .get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

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

        Map<String, Object> doc = client().prepareGet("test", "type", "1")
                .get().getSourceAsMap();
        assertThat(doc.get("val"), equalTo(123.42));
        assertThat(doc.get("status"), equalTo(400));
        assertThat(doc.get("msg"), equalTo("foo"));

        client().prepareBulk().add(
                client().prepareIndex("test", "type", "2").setSource("field1", "123.42 400 <foo>")
        ).putHeader(IngestPlugin.PIPELINE_ID_PARAM, "_id").get();
        doc = client().prepareGet("test", "type", "2").get().getSourceAsMap();
        assertThat(doc.get("val"), equalTo(123.42));
        assertThat(doc.get("status"), equalTo(400));
        assertThat(doc.get("msg"), equalTo("foo"));

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
}
