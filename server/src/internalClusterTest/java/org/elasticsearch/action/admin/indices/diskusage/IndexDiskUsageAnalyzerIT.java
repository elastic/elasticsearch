/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IndexDiskUsageAnalyzerIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EngineTestPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    private static final Set<ShardId> failOnFlushShards = Sets.newConcurrentHashSet();

    public static class EngineTestPlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
                    final ShardId shardId = config.getShardId();
                    if (failOnFlushShards.contains(shardId)) {
                        throw new EngineException(shardId, "simulated IO");
                    }
                    super.flush(force, waitIfOngoing);
                }
            });
        }
    }

    @Before
    public void resetFailOnFlush() throws Exception {
        failOnFlushShards.clear();
    }

    public void testSimple() throws Exception {
        final XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("english_text");
                    mapping.field("type", "text");
                    mapping.endObject();

                    mapping.startObject("value");
                    mapping.field("type", "long");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        final String index = "test-index";
        client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping(mapping)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();

        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            int value = randomIntBetween(1, 1024);
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", English.intToEnglish(value))
                .field("value", value)
                .endObject();
            client().prepareIndex(index).setId("id-" + i).setSource(doc).get();
        }
        final boolean forceNorms = randomBoolean();
        if (forceNorms) {
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", "A long sentence to make sure that norms is non-zero")
                .endObject();
            client().prepareIndex(index).setId("id").setSource(doc).get();
        }
        PlainActionFuture<AnalyzeIndexDiskUsageResponse> future = PlainActionFuture.newFuture();
        client().execute(
            AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] { index }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true),
            future
        );

        AnalyzeIndexDiskUsageResponse resp = future.actionGet();
        final IndexDiskUsageStats stats = resp.getStats().get(index);
        logger.info("--> stats {}", stats);
        assertNotNull(stats);
        assertThat(stats.getIndexSizeInBytes(), greaterThan(100L));

        final IndexDiskUsageStats.PerFieldDiskUsage englishField = stats.getFields().get("english_text");
        assertThat(englishField.getInvertedIndexBytes(), greaterThan(0L));
        assertThat(englishField.getStoredFieldBytes(), equalTo(0L));
        if (forceNorms) {
            assertThat(englishField.getNormsBytes(), greaterThan(0L));
        }
        final IndexDiskUsageStats.PerFieldDiskUsage valueField = stats.getFields().get("value");
        assertThat(valueField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(valueField.getStoredFieldBytes(), equalTo(0L));
        assertThat(valueField.getPointsBytes(), greaterThan(0L));
        assertThat(valueField.getDocValuesBytes(), greaterThan(0L));

        assertMetadataFields(stats);
    }

    public void testGeoShape() throws Exception {
        final XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("location");
                    mapping.field("type", "geo_shape");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        final String index = "test-index";
        client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping(mapping)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();

        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("location")
                .field("type", "point")
                .field("coordinates", new double[] { GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude() })
                .endObject()
                .endObject();
            client().prepareIndex(index).setId("id-" + i).setSource(doc).get();
        }
        AnalyzeIndexDiskUsageResponse resp = client().execute(
            AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] { index }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();

        final IndexDiskUsageStats stats = resp.getStats().get(index);
        logger.info("--> stats {}", stats);
        assertNotNull(stats);
        assertThat(stats.getIndexSizeInBytes(), greaterThan(100L));

        final IndexDiskUsageStats.PerFieldDiskUsage locationField = stats.getFields().get("location");
        assertThat(locationField.totalBytes(), greaterThan(0L));
        assertThat(locationField.getPointsBytes(), greaterThan(0L));
        assertMetadataFields(stats);
    }

    public void testFailOnFlush() throws Exception {
        final String indexName = "test-index";
        int numberOfShards = between(1, 5);
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            )
            .get();
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            int value = randomIntBetween(1, 10);
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", English.intToEnglish(value))
                .field("value", value)
                .endObject();
            client().prepareIndex(indexName).setId("id-" + i).setSource(doc).get();
        }
        Index index = clusterService().state().metadata().index(indexName).getIndex();
        List<ShardId> failedShards = randomSubsetOf(
            between(1, numberOfShards),
            IntStream.range(0, numberOfShards).mapToObj(n -> new ShardId(index, n)).toList()
        );
        failOnFlushShards.addAll(failedShards);
        AnalyzeIndexDiskUsageResponse resp = client().execute(
            AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] { indexName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();
        assertThat(resp.getTotalShards(), equalTo(numberOfShards));
        assertThat(resp.getFailedShards(), equalTo(failedShards.size()));
    }

    public void testManyShards() throws Exception {
        List<String> indices = IntStream.range(0, between(1, 5)).mapToObj(n -> "index_" + n).toList();
        int totalShards = 0;
        for (String indexName : indices) {
            int numberOfShards = between(10, 30);
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
                        .put("index.shard.check_on_startup", false)
                        .put("index.routing.rebalance.enable", "none")
                )
                .get();
            totalShards += numberOfShards;
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                int value = randomIntBetween(5, 20);
                final XContentBuilder doc = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("english_text", English.intToEnglish(value))
                    .field("value", value)
                    .endObject();
                client().prepareIndex(indexName).setId("id-" + i).setSource(doc).get();
            }
        }

        AnalyzeIndexDiskUsageResponse resp = client().execute(
            AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] { "index_*" }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();
        assertThat(Arrays.toString(resp.getShardFailures()), resp.getShardFailures(), emptyArray());
        assertThat(resp.getTotalShards(), equalTo(totalShards));
        assertThat(resp.getSuccessfulShards(), equalTo(totalShards));
        assertThat(resp.getFailedShards(), equalTo(0));
        for (String index : indices) {
            IndexDiskUsageStats stats = resp.getStats().get(index);
            assertThat(stats.getIndexSizeInBytes(), greaterThan(0L));
            assertThat(stats.total().totalBytes(), greaterThan(0L));
        }
    }

    public void testFailingTargetShards() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String indexName = "test-index";
        int numberOfShards = between(1, 5);
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put("index.routing.rebalance.enable", "none")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            int value = randomIntBetween(1, 10);
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", English.intToEnglish(value))
                .field("value", value)
                .endObject();
            client().prepareIndex(indexName).setId("id-" + i).setSource(doc).get();
        }
        final Index index = resolveIndex(indexName);
        final List<ShardId> failingShards = randomSubsetOf(
            between(1, numberOfShards),
            IntStream.range(0, numberOfShards).mapToObj(n -> new ShardId(index, n)).toList()
        );
        final AtomicInteger failedShards = new AtomicInteger();
        final AtomicInteger successfulShards = new AtomicInteger();
        try {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
                transportService.addRequestHandlingBehavior(AnalyzeIndexDiskUsageAction.NAME + "[s]", (handler, request, channel, task) -> {
                    AnalyzeDiskUsageShardRequest shardRequest = (AnalyzeDiskUsageShardRequest) request;
                    IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
                    logger.info("--> handling shard request {} on node {}", shardRequest.shardId(), node);
                    ShardId shardId = shardRequest.shardId();
                    if (failingShards.contains(shardId)) {
                        IndexShard indexShard = indicesService.getShardOrNull(shardId);
                        assertNotNull("No shard found for shard " + shardId, indexShard);
                        logger.info("--> failing shard {} on node {}", shardRequest.shardId(), node);
                        indexShard.close("test", randomBoolean());
                        failedShards.incrementAndGet();
                    } else {
                        successfulShards.incrementAndGet();
                    }
                    handler.messageReceived(request, channel, task);
                });
            }
            AnalyzeIndexDiskUsageResponse resp = client().execute(
                AnalyzeIndexDiskUsageAction.INSTANCE,
                new AnalyzeIndexDiskUsageRequest(new String[] { indexName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
            ).actionGet();
            assertThat(failedShards.get(), equalTo(failingShards.size()));
            assertThat(resp.getTotalShards(), equalTo(numberOfShards));
            assertThat(resp.getFailedShards(), equalTo(failedShards.get()));
            assertThat(resp.getSuccessfulShards(), equalTo(resp.getTotalShards() - resp.getFailedShards()));
            assertThat(successfulShards.get(), equalTo(numberOfShards - failedShards.get()));
            assertThat(resp.getShardFailures(), arrayWithSize(failedShards.get()));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
                transportService.clearAllRules();
            }
        }
    }

    void assertMetadataFields(IndexDiskUsageStats stats) {
        final IndexDiskUsageStats.PerFieldDiskUsage sourceField = stats.getFields().get("_source");
        assertThat(sourceField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(sourceField.getStoredFieldBytes(), greaterThan(0L));
        assertThat(sourceField.getPointsBytes(), equalTo(0L));
        assertThat(sourceField.getDocValuesBytes(), equalTo(0L));

        final IndexDiskUsageStats.PerFieldDiskUsage idField = stats.getFields().get("_id");
        assertThat(idField.getInvertedIndexBytes(), greaterThan(0L));
        assertThat(idField.getStoredFieldBytes(), greaterThan(0L));
        assertThat(idField.getPointsBytes(), equalTo(0L));
        assertThat(idField.getDocValuesBytes(), equalTo(0L));

        final IndexDiskUsageStats.PerFieldDiskUsage seqNoField = stats.getFields().get("_seq_no");
        assertThat(seqNoField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(seqNoField.getStoredFieldBytes(), equalTo(0L));
        assertThat(seqNoField.getPointsBytes(), greaterThan(0L));
        assertThat(seqNoField.getDocValuesBytes(), greaterThan(0L));
    }
}
