/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // expose HTTP requests
    }

    public void testCreationDateGivenFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, 4L)).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "unknown setting [index.creation_date] please check that any required plugins are installed, or check the "
                    + "breaking changes documentation for removed settings",
                ex.getMessage()
            );
        }
    }

    public void testCreationDateGenerated() {
        long timeBeforeRequest = System.currentTimeMillis();
        prepareCreate("test").get();
        long timeAfterRequest = System.currentTimeMillis();
        ClusterStateResponse response = clusterAdmin().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        Metadata metadata = state.getMetadata();
        assertThat(metadata, notNullValue());
        Map<String, IndexMetadata> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetadata index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.getCreationDate(), allOf(lessThanOrEqualTo(timeAfterRequest), greaterThanOrEqualTo(timeBeforeRequest)));
    }

    public void testNonNestedMappings() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("date")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        GetMappingsResponse response = indicesAdmin().prepareGetMappings("test").get();

        MappingMetadata mappings = response.mappings().get("test");
        assertNotNull(mappings);
        assertFalse(mappings.sourceAsMap().isEmpty());
    }

    public void testEmptyNestedMappings() throws Exception {
        assertAcked(prepareCreate("test").setMapping(XContentFactory.jsonBuilder().startObject().endObject()));

        GetMappingsResponse response = indicesAdmin().prepareGetMappings("test").get();

        MappingMetadata mappings = response.mappings().get("test");
        assertNotNull(mappings);
        assertTrue(mappings.sourceAsMap().isEmpty());
    }

    public void testMappingParamAndNestedMismatch() throws Exception {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            prepareCreate("test").setMapping(XContentFactory.jsonBuilder().startObject().startObject("type2").endObject().endObject())
        );
        assertThat(e.getMessage(), startsWith("Failed to parse mapping: Root mapping definition has unsupported parameters"));
    }

    public void testEmptyMappings() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject())
        );

        GetMappingsResponse response = indicesAdmin().prepareGetMappings("test").get();

        MappingMetadata mappings = response.mappings().get("test");
        assertNotNull(mappings);
        assertTrue(mappings.sourceAsMap().isEmpty());
    }

    public void testInvalidShardCountSettings() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, value).build()).get();
            fail("should have thrown an exception about the primary shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, value).build()).get();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }

    }

    public void testCreateIndexWithBlocks() {
        try {
            setClusterReadOnly(true);
            assertBlocked(prepareCreate("test"));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testCreateIndexWithMetadataBlocks() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_METADATA, true)));
        assertBlocked(indicesAdmin().prepareGetSettings("test"), IndexMetadata.INDEX_METADATA_BLOCK);
        disableIndexBlock("test", IndexMetadata.SETTING_BLOCKS_METADATA);
    }

    public void testUnknownSettingFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put("index.unknown.value", "this must fail").build()).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals(
                "unknown setting [index.unknown.value] please check that any required plugins are installed, or check the"
                    + " breaking changes documentation for removed settings",
                e.getMessage()
            );
        }
    }

    public void testInvalidShardCountSettingsWithoutPrefix() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS.substring(IndexMetadata.INDEX_SETTING_PREFIX.length()), value)
                    .build()
            ).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS.substring(IndexMetadata.INDEX_SETTING_PREFIX.length()), value)
                    .build()
            ).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }
    }

    public void testCreateAndDeleteIndexConcurrently() throws InterruptedException {
        createIndex("test");
        final AtomicInteger indexVersion = new AtomicInteger(0);
        final Object indexVersionLock = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setSource("index_version", indexVersion.get()).get();
        }
        synchronized (indexVersionLock) { // not necessarily needed here but for completeness we lock here too
            indexVersion.incrementAndGet();
        }
        indicesAdmin().prepareDelete("test").execute(new ActionListener<AcknowledgedResponse>() { // this happens async!!!
            @Override
            public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            // recreate that index
                            prepareIndex("test").setSource("index_version", indexVersion.get()).get();
                            synchronized (indexVersionLock) {
                                // we sync here since we have to ensure that all indexing operations below for a given ID are done before
                                // we increment the index version otherwise a doc that is in-flight could make it into an index that it
                                // was supposed to be deleted for and our assertion fail...
                                indexVersion.incrementAndGet();
                            }
                            // from here on all docs with index_version == 0|1 must be gone!!!! only 2 are ok;
                            assertAcked(indicesAdmin().prepareDelete("test").get());
                        } finally {
                            latch.countDown();
                        }
                    }
                };
                thread.start();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });
        numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            try {
                synchronized (indexVersionLock) {
                    prepareIndex("test").setSource("index_version", indexVersion.get()).setTimeout(TimeValue.timeValueSeconds(10)).get();
                }
            } catch (IndexNotFoundException inf) {
                // fine
            } catch (UnavailableShardsException ex) {
                assertEquals(ex.getCause().getClass(), IndexNotFoundException.class);
                // fine we run into a delete index while retrying
            }
        }
        latch.await();
        refresh();

        // we only really assert that we never reuse segments of old indices or anything like this here and that nothing fails with
        // crazy exceptions
        assertNoFailuresAndResponse(
            prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(new RangeQueryBuilder("index_version").from(indexVersion.get(), true)),
            expected -> assertNoFailuresAndResponse(prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()), all -> {
                assertEquals(expected + " vs. " + all, expected.getHits().getTotalHits().value, all.getHits().getTotalHits().value);
                logger.info("total: {}", expected.getHits().getTotalHits().value);
            })
        );
    }

    public void testRestartIndexCreationAfterFullClusterRestart() throws Exception {
        clusterAdmin().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"))
            .get();
        indicesAdmin().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(indexSettings()).get();
        internalCluster().fullRestart();
        ensureGreen("test");
    }

    public void testFailureToCreateIndexCleansUpIndicesService() {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = indexSettings(1, numReplicas).build();
        assertAcked(indicesAdmin().prepareCreate("test-idx-1").setSettings(settings).addAlias(new Alias("alias1").writeIndex(true)).get());

        ActionRequestBuilder<?, ?> builder = indicesAdmin().prepareCreate("test-idx-2")
            .setSettings(settings)
            .addAlias(new Alias("alias1").writeIndex(true));
        expectThrows(IllegalStateException.class, builder);

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, internalCluster().getMasterName());
        for (IndexService indexService : indicesService) {
            assertThat(indexService.index().getName(), not("test-idx-2"));
        }
    }

    /**
     * This test ensures that index creation adheres to the {@link IndexMetadata#SETTING_WAIT_FOR_ACTIVE_SHARDS}.
     */
    public void testDefaultWaitForActiveShardsUsesIndexSetting() throws Exception {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = indexSettings(1, numReplicas).put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas))
            .build();
        assertAcked(indicesAdmin().prepareCreate("test-idx-1").setSettings(settings).get());

        // all should fail
        settings = Settings.builder().put(settings).put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "all").build();
        assertFalse(
            indicesAdmin().prepareCreate("test-idx-2")
                .setSettings(settings)
                .setTimeout(TimeValue.timeValueMillis(100))
                .get()
                .isShardsAcknowledged()
        );

        // the numeric equivalent of all should also fail
        settings = Settings.builder().put(settings).put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas + 1)).build();
        assertFalse(
            indicesAdmin().prepareCreate("test-idx-3")
                .setSettings(settings)
                .setTimeout(TimeValue.timeValueMillis(100))
                .get()
                .isShardsAcknowledged()
        );
    }

    public void testInvalidPartitionSize() {
        BiFunction<Integer, Integer, Boolean> createPartitionedIndex = (shards, partitionSize) -> {
            CreateIndexResponse response;

            try {
                response = prepareCreate("test_" + shards + "_" + partitionSize).setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_routing_shards", shards)
                        .put("index.routing_partition_size", partitionSize)
                ).get();
            } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
            }

            return response.isAcknowledged();
        };

        assertFalse(createPartitionedIndex.apply(3, 6));
        assertFalse(createPartitionedIndex.apply(3, 0));
        assertFalse(createPartitionedIndex.apply(3, 3));

        assertTrue(createPartitionedIndex.apply(3, 1));
        assertTrue(createPartitionedIndex.apply(3, 2));

        assertTrue(createPartitionedIndex.apply(1, 1));
    }

    public void testIndexNameInResponse() {
        CreateIndexResponse response = prepareCreate("foo").setSettings(Settings.builder().build()).get();

        assertEquals("Should have index name in response", "foo", response.index());
    }

    public void testInfiniteAckTimeout() throws IOException {
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var barrier = new CyclicBarrier(2);
        clusterService.getClusterApplierService().runOnApplierThread("block for test", Priority.NORMAL, cs -> {
            safeAwait(barrier);
            safeAwait(barrier);
        }, ActionListener.noop());

        safeAwait(barrier);

        final var request = ESRestTestCase.newXContentRequest(
            HttpMethod.PUT,
            "testindex",
            (builder, params) -> builder.startObject("settings")
                .field(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .field(SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 1)
                .endObject()
        );
        request.addParameter("timeout", "-1");
        final var responseFuture = new PlainActionFuture<Response>();
        getRestClient().performRequestAsync(request, ActionTestUtils.wrapAsRestResponseListener(responseFuture));

        if (randomBoolean()) {
            safeSleep(scaledRandomIntBetween(1, 100));
        }

        assertFalse(responseFuture.isDone());
        safeAwait(barrier);

        final var response = FutureUtils.get(responseFuture, 10, TimeUnit.SECONDS);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertTrue((boolean) extractValue("acknowledged", entityAsMap(response)));
    }

}
