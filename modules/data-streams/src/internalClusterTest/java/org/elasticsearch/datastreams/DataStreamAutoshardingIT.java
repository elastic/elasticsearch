/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamAutoshardingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, MockTransportService.TestPlugin.class, TestAutoshardingPlugin.class);
    }

    @Before
    public void configureClusterSettings() {
        updateClusterSettings(
            Settings.builder().putList(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.getKey(), List.of())
        );
    }

    @After
    public void resetClusterSetting() {
        updateClusterSettings(
            Settings.builder().putNull(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.getKey())
        );
    }

    public void testRolloverOnAutoShardCondition() throws Exception {
        final String dataStreamName = "logs-es";

        putComposableIndexTemplate("my-template", null, List.of("logs-*"), Settings.EMPTY);
        final var createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        for (int i = 0; i < 10; i++) {
            indexDocs(dataStreamName, randomIntBetween(100, 200));
        }

        final ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata().dataStreams().get(dataStreamName);
        final String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
            .index(dataStreamBeforeRollover.getWriteIndex())
            .shard(0)
            .primaryShard()
            .currentNodeId();

        Index writeIndex = clusterStateBeforeRollover.metadata().dataStreams().get(dataStreamName).getWriteIndex();
        IndexMetadata indexMeta = clusterStateBeforeRollover.getMetadata().index(writeIndex);
        ShardId shardId = new ShardId(indexMeta.getIndex(), 0);
        Path path = createTempDir().resolve("indices").resolve(indexMeta.getIndexUUID()).resolve(String.valueOf(0));
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        shardRouting = shardRouting.initialize(assignedShardNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        CommonStats stats = new CommonStats();
        stats.docs = new DocsStats();
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats(new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, false, 1, 4, 1));
        final ShardStats shardStats = new ShardStats(
            shardRouting,
            new ShardPath(false, path, path, shardId),
            stats,
            null,
            null,
            null,
            false,
            0
        );

        for (DiscoveryNode node : clusterStateBeforeRollover.nodes().getAllNodes()) {
            MockTransportService.getInstance(node.getName())
                .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                    TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, node.getName());
                    channel.sendResponse(instance.new NodeResponse(node.getId(), 1, List.of(shardStats), List.of()));
                });
        }

        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

        final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);
        final IndexMetadata currentWriteIndexMetadata = clusterState.metadata().getIndexSafe(dataStream.getWriteIndex());

        assertThat(currentWriteIndexMetadata.getNumberOfShards(), is(3));
    }

    static void putComposableIndexTemplate(String id, @Nullable String mappings, List<String> patterns, @Nullable Settings settings)
        throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(new Template(settings, mappings == null ? null : CompressedXContent.fromJSON(mappings), null, null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    /**
     * Test plugin that registers an additional setting.
     */
    public static class TestAutoshardingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.boolSetting(DATA_STREAMS_AUTO_SHARDING_ENABLED, false, Setting.Property.Dynamic, Setting.Property.NodeScope)
            );
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(DATA_STREAMS_AUTO_SHARDING_ENABLED, true).build();
        }
    }

}
