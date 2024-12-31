/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestPipelineTestUtils.jsonSimulatePipelineRequest;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.putJsonPipelineRequest;
import static org.elasticsearch.test.NodeRoles.nonIngestNode;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IngestClientIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (nodeOrdinal % 2 == 0) {
            return Settings.builder().put(nonIngestNode()).put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
        }
        return super.nodeSettings(nodeOrdinal, otherSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ExtendedIngestTestPlugin.class);
    }

    public void testSimulate() throws Exception {
        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
        );
        GetPipelineResponse getResponse = getPipelines("_id");
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        BytesReference bytes = BytesReference.bytes(
            jsonBuilder().startObject()
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
                .endObject()
        );
        SimulatePipelineResponse response;
        if (randomBoolean()) {
            response = clusterAdmin().prepareSimulatePipeline(bytes, XContentType.JSON).setId("_id").get();
        } else {
            SimulatePipelineRequest request = jsonSimulatePipelineRequest(bytes);
            request.setId("_id");
            response = clusterAdmin().simulatePipeline(request).get();
        }
        assertThat(response.isVerbose(), equalTo(false));
        assertThat(response.getPipelineId(), equalTo("_id"));
        assertThat(response.getResults().size(), equalTo(1));
        assertThat(response.getResults().get(0), instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) response.getResults().get(0);
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        source.put("fail", false);
        source.put("processed", true);
        IngestDocument ingestDocument = new IngestDocument("index", "id", Versions.MATCH_ANY, null, null, source);
        assertThat(simulateDocumentBaseResult.getIngestDocument().getSource(), equalTo(ingestDocument.getSource()));
        assertThat(simulateDocumentBaseResult.getIngestDocument().getMetadata().getMap(), equalTo(ingestDocument.getMetadata().getMap()));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());

        // cleanup
        deletePipeline("_id");
    }

    public void testBulkWithIngestFailures() throws Exception {
        createIndex("index");

        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
        );

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index").id(Integer.toString(i)).setPipeline("_id");
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", i % 2 == 0);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (i % 2 == 0) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                ElasticsearchException compoundProcessorException = (ElasticsearchException) failure.getCause();
                assertThat(compoundProcessorException.getRootCause().getMessage(), equalTo("test processor failed"));
            } else {
                IndexResponse indexResponse = itemResponse.getResponse();
                assertThat(
                    "Expected a successful response but found failure [" + itemResponse.getFailure() + "].",
                    itemResponse.isFailed(),
                    is(false)
                );
                assertThat(indexResponse, notNullValue());
                assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
            }
        }

        // cleanup
        deletePipeline("_id");
    }

    public void testBulkWithUpsert() throws Exception {
        createIndex("index");

        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
        );

        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index").id("1").setPipeline("_id");
        indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "val1");
        bulkRequest.add(indexRequest);
        UpdateRequest updateRequest = new UpdateRequest("index", "2");
        updateRequest.doc("{}", Requests.INDEX_CONTENT_TYPE);
        updateRequest.upsert("{\"field1\":\"upserted_val\"}", XContentType.JSON).upsertRequest().setPipeline("_id");
        bulkRequest.add(updateRequest);

        BulkResponse response = client().bulk(bulkRequest).actionGet();

        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        Map<String, Object> inserted = client().prepareGet("index", "1").get().getSourceAsMap();
        assertThat(inserted.get("field1"), equalTo("val1"));
        assertThat(inserted.get("processed"), equalTo(true));
        Map<String, Object> upserted = client().prepareGet("index", "2").get().getSourceAsMap();
        assertThat(upserted.get("field1"), equalTo("upserted_val"));
        assertThat(upserted.get("processed"), equalTo(true));
    }

    public void test() throws Exception {
        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
        );

        GetPipelineResponse getResponse = getPipelines("_id");
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        prepareIndex("test").setId("1").setPipeline("_id").setSource("field", "value", "fail", false).get();

        Map<String, Object> doc = client().prepareGet("test", "1").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value"));
        assertThat(doc.get("processed"), equalTo(true));

        client().prepareBulk().add(prepareIndex("test").setId("2").setSource("field", "value2", "fail", false).setPipeline("_id")).get();
        doc = client().prepareGet("test", "2").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value2"));
        assertThat(doc.get("processed"), equalTo(true));

        deletePipeline("_id");

        getResponse = getPipelines("_id");
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    public void testPutWithPipelineFactoryError() throws Exception {
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .field("unused", ":sad_face:")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = putJsonPipelineRequest("_id2", source);
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            client().execute(PutPipelineTransportAction.TYPE, putPipelineRequest)
        );
        assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));

        GetPipelineResponse response = getPipelines("_id2");
        assertFalse(response.isFound());
    }

    public void testWithDedicatedMaster() throws Exception {
        String masterOnlyNode = internalCluster().startMasterOnlyNode();
        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
        );

        BulkItemResponse item = client(masterOnlyNode).prepareBulk()
            .add(prepareIndex("test").setSource("field", "value2", "drop", true).setPipeline("_id"))
            .get()
            .getItems()[0];
        assertFalse(item.isFailed());
        assertEquals("auto-generated", item.getResponse().getId());
    }

    public void testPipelineOriginHeader() throws Exception {
        putJsonPipeline("1", (source, params) -> {
            source.startArray("processors");
            source.startObject();
            {
                source.startObject("pipeline");
                source.field("name", "2");
                source.endObject();
            }
            source.endObject();
            return source.endArray();
        });
        putJsonPipeline("2", (source, params) -> {
            source.startArray("processors");
            source.startObject();
            {
                source.startObject("pipeline");
                source.field("name", "3");
                source.endObject();
            }
            source.endObject();
            return source.endArray();
        });
        putJsonPipeline("3", (source, params) -> {
            source.startArray("processors");
            source.startObject();
            {
                source.startObject("fail");
                source.endObject();
            }
            source.endObject();
            return source.endArray();
        });

        Exception e = expectThrows(Exception.class, () -> {
            IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.source("{}", XContentType.JSON);
            indexRequest.setPipeline("1");
            client().index(indexRequest).get();
        });
        IngestProcessorException ingestException = (IngestProcessorException) e.getCause();
        assertThat(ingestException.getHeader("processor_type"), equalTo(List.of("fail")));
        assertThat(ingestException.getHeader("pipeline_origin"), equalTo(List.of("3", "2", "1")));
    }

    public void testPipelineProcessorOnFailure() throws Exception {
        putJsonPipeline("1", (source, params) -> {
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "2");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            {
                source.startArray("on_failure");
                source.startObject();
                {
                    source.startObject("onfailure_processor");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            return source;
        });
        putJsonPipeline("2", (source, params) -> {
            source.startArray("processors");
            source.startObject();
            {
                source.startObject("pipeline");
                source.field("name", "3");
                source.endObject();
            }
            source.endObject();
            return source.endArray();
        });
        putJsonPipeline("3", (source, params) -> {
            source.startArray("processors");
            source.startObject();
            {
                source.startObject("fail");
                source.endObject();
            }
            source.endObject();
            return source.endArray();
        });

        prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).setPipeline("1").get();
        Map<String, Object> inserted = client().prepareGet("test", "1").get().getSourceAsMap();
        assertThat(inserted.get("readme"), equalTo("pipeline with id [3] is a bad pipeline"));
    }

    public static class ExtendedIngestTestPlugin extends IngestTestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> factories = new HashMap<>(super.getProcessors(parameters));
            factories.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService));
            factories.put(
                "fail",
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, new RuntimeException())
            );
            factories.put(
                "onfailure_processor",
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, document -> {
                    String onFailurePipeline = document.getFieldValue("_ingest.on_failure_pipeline", String.class);
                    document.setFieldValue("readme", "pipeline with id [" + onFailurePipeline + "] is a bad pipeline");
                })
            );
            return factories;
        }
    }

}
