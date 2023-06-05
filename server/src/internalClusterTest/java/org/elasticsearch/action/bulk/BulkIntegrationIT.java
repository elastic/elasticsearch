/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class BulkIntegrationIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestTestPlugin.class);
    }

    public void testBulkIndexCreatesMapping() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/bulk-log.json");
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        bulkBuilder.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        bulkBuilder.get();
        assertBusy(() -> {
            GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings().get();
            assertTrue(mappingsResponse.getMappings().containsKey("logstash-2014.03.30"));
        });
    }

    /**
     * This tests that the {@link TransportBulkAction} evaluates alias routing values correctly when dealing with
     * an alias pointing to multiple indices, while a write index exits.
     */
    public void testBulkWithWriteIndexAndRouting() {
        Map<String, Integer> twoShardsSettings = Collections.singletonMap(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2);
        indicesAdmin().prepareCreate("index1").addAlias(new Alias("alias1").indexRouting("0")).setSettings(twoShardsSettings).get();
        indicesAdmin().prepareCreate("index2")
            .addAlias(new Alias("alias1").indexRouting("0").writeIndex(randomFrom(false, null)))
            .setSettings(twoShardsSettings)
            .get();
        indicesAdmin().prepareCreate("index3")
            .addAlias(new Alias("alias1").indexRouting("1").writeIndex(true))
            .setSettings(twoShardsSettings)
            .get();

        IndexRequest indexRequestWithAlias = new IndexRequest("alias1").id("id");
        if (randomBoolean()) {
            indexRequestWithAlias.routing("1");
        }
        indexRequestWithAlias.source(Collections.singletonMap("foo", "baz"));
        BulkResponse bulkResponse = client().prepareBulk().add(indexRequestWithAlias).get();
        assertThat(bulkResponse.getItems()[0].getResponse().getIndex(), equalTo("index3"));
        assertThat(bulkResponse.getItems()[0].getResponse().getShardId().getId(), equalTo(0));
        assertThat(bulkResponse.getItems()[0].getResponse().getVersion(), equalTo(1L));
        assertThat(bulkResponse.getItems()[0].getResponse().status(), equalTo(RestStatus.CREATED));
        assertThat(client().prepareGet("index3", "id").setRouting("1").get().getSource().get("foo"), equalTo("baz"));

        bulkResponse = client().prepareBulk().add(client().prepareUpdate("alias1", "id").setDoc("foo", "updated")).get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        assertThat(client().prepareGet("index3", "id").setRouting("1").get().getSource().get("foo"), equalTo("updated"));
        bulkResponse = client().prepareBulk().add(client().prepareDelete("alias1", "id")).get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        assertFalse(client().prepareGet("index3", "id").setRouting("1").get().isExists());
    }

    // allowing the auto-generated timestamp to externally be set would allow making the index inconsistent with duplicate docs
    public void testExternallySetAutoGeneratedTimestamp() {
        IndexRequest indexRequest = new IndexRequest("index1").source(Collections.singletonMap("foo", "baz"));
        indexRequest.autoGenerateId();
        if (randomBoolean()) {
            indexRequest.id("test");
        }
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> client().prepareBulk().add(indexRequest).get()).getMessage(),
            containsString("autoGeneratedTimestamp should not be set externally")
        );
    }

    public void testBulkWithGlobalDefaults() throws Exception {
        // all requests in the json are missing index and type parameters: "_index" : "test", "_type" : "type1",
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk-missing-index-type.json");
        {
            BulkRequestBuilder bulkBuilder = client().prepareBulk();
            bulkBuilder.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
            ActionRequestValidationException ex = expectThrows(ActionRequestValidationException.class, bulkBuilder::get);

            assertThat(ex.validationErrors(), containsInAnyOrder("index is missing", "index is missing", "index is missing"));
        }

        {
            createSamplePipeline("pipeline");
            BulkRequestBuilder bulkBuilder = client().prepareBulk("test").routing("routing").pipeline("pipeline");

            bulkBuilder.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
            BulkResponse bulkItemResponses = bulkBuilder.get();
            assertFalse(bulkItemResponses.hasFailures());
        }
    }

    private void createSamplePipeline(String pipelineId) throws IOException, ExecutionException, InterruptedException {
        XContentBuilder pipeline = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        AcknowledgedResponse acknowledgedResponse = clusterAdmin().putPipeline(
            new PutPipelineRequest(pipelineId, BytesReference.bytes(pipeline), XContentType.JSON)
        ).get();

        assertTrue(acknowledgedResponse.isAcknowledged());
    }

    /** This test ensures that index deletion makes indexing fail quickly, not wait on the index that has disappeared */
    public void testDeleteIndexWhileIndexing() throws Exception {
        String index = "deleted_while_indexing";
        createIndex(index);
        AtomicBoolean stopped = new AtomicBoolean();
        Thread[] threads = new Thread[between(1, 4)];
        AtomicInteger docID = new AtomicInteger();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < 5000) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        IndexResponse response = client().prepareIndex(index)
                            .setId(id)
                            .setSource(Map.of("f" + randomIntBetween(1, 10), randomNonNegativeLong()), XContentType.JSON)
                            .get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                        logger.info("--> index id={} seq_no={}", response.getId(), response.getSeqNo());
                    } catch (ElasticsearchException ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }
        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(1)));
        assertAcked(client().admin().indices().prepareDelete(index));
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join(ReplicationRequest.DEFAULT_TIMEOUT.millis() / 2);
            assertFalse(thread.isAlive());
        }
    }

}
