/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.action.admin.indices.rollover.OptimalShardCountCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamAutoshardingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            MockTransportService.TestPlugin.class,
            TestAutoshardingPlugin.class,
            TestTelemetryPlugin.class
        );
    }

    @Before
    public void configureClusterSettings() {
        updateClusterSettings(
            Settings.builder()
                // we want to manually trigger the rollovers in this test suite to be able to assert incrementally the changes in shard
                // configurations
                .put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "30d")
        );
    }

    @After
    public void resetClusterSetting() {
        updateClusterSettings(Settings.builder().putNull(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL));
    }

    public void testRolloverOnAutoShardCondition() throws Exception {
        final String dataStreamName = "logs-es";

        putComposableIndexTemplate("my-template", List.of("logs-*"), indexSettings(3, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        indexDocs(dataStreamName, randomIntBetween(100, 200));

        {
            resetTelemetry();
            ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                .index(dataStreamBeforeRollover.getWriteIndex())
                .shard(0)
                .primaryShard()
                .currentNodeId();

            Index firstGenerationIndex = clusterStateBeforeRollover.metadata()
                .getProject()
                .dataStreams()
                .get(dataStreamName)
                .getWriteIndex();
            IndexMetadata firstGenerationMeta = clusterStateBeforeRollover.getMetadata().getProject().index(firstGenerationIndex);

            List<ShardStats> shards = new ArrayList<>(firstGenerationMeta.getNumberOfShards());
            for (int i = 0; i < firstGenerationMeta.getNumberOfShards(); i++) {
                // the shard stats will yield a write load of 75.0 which will make the auto sharding service recommend an optimal number
                // of 5 shards
                shards.add(
                    getShardStats(
                        firstGenerationMeta,
                        i,
                        (long) Math.ceil(75.0 / firstGenerationMeta.getNumberOfShards()),
                        assignedShardNodeId
                    )
                );
            }

            mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, firstGenerationMeta, shards);
            assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

            ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            IndexMetadata secondGenerationMeta = clusterStateAfterRollover.metadata().getProject().getIndexSafe(dataStream.getWriteIndex());

            // we auto sharded up to 5 shards
            assertThat(secondGenerationMeta.getNumberOfShards(), is(5));

            IndexMetadata index = clusterStateAfterRollover.metadata().getProject().index(firstGenerationIndex);
            Map<String, RolloverInfo> rolloverInfos = index.getRolloverInfos();
            assertThat(rolloverInfos.size(), is(1));
            List<Condition<?>> metConditions = rolloverInfos.get(dataStreamName).getMetConditions();
            assertThat(metConditions.size(), is(1));
            assertThat(metConditions.get(0).value(), instanceOf(Integer.class));
            int autoShardingRolloverInfo = (int) metConditions.get(0).value();
            assertThat(autoShardingRolloverInfo, is(5));

            assertTelemetry(MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.get(AutoShardingType.INCREASE_SHARDS));
        }

        // let's do another rollover now that will not increase the number of shards because the increase shards cooldown has not lapsed,
        // however the rollover will use the existing/previous auto shard configuration and the new generation index will have 5 shards
        {
            resetTelemetry();
            ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                .index(dataStreamBeforeRollover.getWriteIndex())
                .shard(0)
                .primaryShard()
                .currentNodeId();

            IndexMetadata secondGenerationMeta = clusterStateBeforeRollover.metadata()
                .getProject()
                .index(dataStreamBeforeRollover.getIndices().get(1));
            List<ShardStats> shards = new ArrayList<>(secondGenerationMeta.getNumberOfShards());
            for (int i = 0; i < secondGenerationMeta.getNumberOfShards(); i++) {
                // the shard stats will yield a write load of 100.0 which will make the auto sharding service recommend an optimal number of
                // 7 shards
                shards.add(
                    getShardStats(
                        secondGenerationMeta,
                        i,
                        (long) Math.ceil(100.0 / secondGenerationMeta.getNumberOfShards()),
                        assignedShardNodeId
                    )
                );
            }
            mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, secondGenerationMeta, shards);

            RolloverResponse response = indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet();
            assertAcked(response);
            Map<String, Boolean> conditionStatus = response.getConditionStatus();
            // empty rollover executed
            assertThat(conditionStatus.size(), is(0));

            ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            IndexMetadata thirdGenerationMeta = clusterStateAfterRollover.metadata().getProject().getIndexSafe(dataStream.getWriteIndex());

            // we remained on 5 shards due to the increase shards cooldown
            assertThat(thirdGenerationMeta.getNumberOfShards(), is(5));

            assertTelemetry(MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.get(AutoShardingType.COOLDOWN_PREVENTED_INCREASE));
        }

        {
            try {
                // eliminate the increase shards cooldown and re-do the rollover should configure the data stream to 7 shards
                // this time also add a rollover condition that does NOT match so that we test that it's the auto sharding that triggers
                // indeed the rollover
                updateClusterSettings(
                    Settings.builder().put(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.getKey(), "0s")
                );

                ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata()
                    .getProject()
                    .dataStreams()
                    .get(dataStreamName);
                String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                    .index(dataStreamBeforeRollover.getWriteIndex())
                    .shard(0)
                    .primaryShard()
                    .currentNodeId();

                IndexMetadata thirdGenIndex = clusterStateBeforeRollover.metadata()
                    .getProject()
                    .index(dataStreamBeforeRollover.getIndices().get(2));
                List<ShardStats> shards = new ArrayList<>(thirdGenIndex.getNumberOfShards());
                for (int i = 0; i < thirdGenIndex.getNumberOfShards(); i++) {
                    // the shard stats will yield a write load of 100.0 which will make the auto sharding service recommend an optimal
                    // number of 7 shards
                    shards.add(
                        getShardStats(thirdGenIndex, i, (long) Math.ceil(100.0 / thirdGenIndex.getNumberOfShards()), assignedShardNodeId)
                    );
                }
                mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, thirdGenIndex, shards);

                RolloverRequest request = new RolloverRequest(dataStreamName, null);
                request.setConditions(RolloverConditions.newBuilder().addMaxIndexDocsCondition(1_000_000L).build());
                RolloverResponse response = indicesAdmin().rolloverIndex(request).actionGet();
                assertAcked(response);
                Map<String, Boolean> conditionStatus = response.getConditionStatus();
                assertThat(conditionStatus.size(), is(2));
                for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
                    if (entry.getKey().equals(new MaxDocsCondition(1_000_000L).toString())) {
                        assertThat(entry.getValue(), is(false));
                    } else {
                        assertThat(entry.getKey(), is(new OptimalShardCountCondition(7).toString()));
                        assertThat(entry.getValue(), is(true));
                    }
                }

                ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
                IndexMetadata fourthGenerationMeta = clusterStateAfterRollover.metadata()
                    .getProject()
                    .getIndexSafe(dataStream.getWriteIndex());

                // we auto-sharded up to 7 shards as there was no cooldown period
                assertThat(fourthGenerationMeta.getNumberOfShards(), is(7));
            } finally {
                // reset increase shards cooldown value
                updateClusterSettings(
                    Settings.builder().putNull(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.getKey())
                );
            }
        }
    }

    public void testReduceShardsOnRollover() throws IOException {
        final String dataStreamName = "logs-es";

        // start with 3 shards
        putComposableIndexTemplate("my-template", List.of("logs-*"), indexSettings(3, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        indexDocs(dataStreamName, randomIntBetween(100, 200));

        {
            // rollover executes but the reduction in shard number will not be executed due to the reduce shards cooldown
            ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                .index(dataStreamBeforeRollover.getWriteIndex())
                .shard(0)
                .primaryShard()
                .currentNodeId();

            Index firstGenerationIndex = clusterStateBeforeRollover.metadata()
                .getProject()
                .dataStreams()
                .get(dataStreamName)
                .getWriteIndex();
            IndexMetadata firstGenerationMeta = clusterStateBeforeRollover.getMetadata().getProject().index(firstGenerationIndex);

            List<ShardStats> shards = new ArrayList<>(firstGenerationMeta.getNumberOfShards());
            for (int i = 0; i < firstGenerationMeta.getNumberOfShards(); i++) {
                // the shard stats will yield a write load of 2.0 which will make the auto sharding service recommend an optimal number
                // of 2 shards
                shards.add(getShardStats(firstGenerationMeta, i, i < 2 ? 1 : 0, assignedShardNodeId));
            }

            mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, firstGenerationMeta, shards);
            assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

            ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            IndexMetadata secondGenerationMeta = clusterStateAfterRollover.metadata().getProject().getIndexSafe(dataStream.getWriteIndex());

            // we kept the number of shards to 3 as the reduce shards cooldown prevented us reducing the number of shards
            assertThat(secondGenerationMeta.getNumberOfShards(), is(3));
        }

        {
            // temporarily disable reduce shards cooldown and test that a rollover that doesn't match ANOTHER condition will not be
            // executed just because we need to reduce the number of shards, and then that rollover when a different condition does
            // indeed match will execute the rollover and the number of shards will be reduced to 2
            try {
                updateClusterSettings(
                    Settings.builder().put(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.getKey(), "0s")
                );

                ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata()
                    .getProject()
                    .dataStreams()
                    .get(dataStreamName);
                String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                    .index(dataStreamBeforeRollover.getWriteIndex())
                    .shard(0)
                    .primaryShard()
                    .currentNodeId();

                IndexMetadata secondGenerationIndex = clusterStateBeforeRollover.metadata()
                    .getProject()
                    .index(dataStreamBeforeRollover.getIndices().get(1));
                List<ShardStats> shards = new ArrayList<>(secondGenerationIndex.getNumberOfShards());
                for (int i = 0; i < secondGenerationIndex.getNumberOfShards(); i++) {
                    // the shard stats will yield a write load of 2.0 which will make the auto sharding service recommend an
                    // optimal number of 2 shards
                    shards.add(getShardStats(secondGenerationIndex, i, i < 2 ? 1 : 0, assignedShardNodeId));
                }
                mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, secondGenerationIndex, shards);

                RolloverRequest request = new RolloverRequest(dataStreamName, null);
                // adding condition that does NOT match
                request.setConditions(RolloverConditions.newBuilder().addMaxIndexDocsCondition(1_000_000L).build());
                RolloverResponse response = indicesAdmin().rolloverIndex(request).actionGet();
                assertThat(response.isRolledOver(), is(false));
                Map<String, Boolean> conditionStatus = response.getConditionStatus();
                assertThat(conditionStatus.size(), is(1));
                assertThat(conditionStatus.get(new MaxDocsCondition(1_000_000L).toString()), is(false));

                // let's rollover with a condition that does match and test that the number of shards is reduced to 2
                indexDocs(dataStreamName, 100);
                request = new RolloverRequest(dataStreamName, null);
                // adding condition that does NOT match
                request.setConditions(RolloverConditions.newBuilder().addMaxIndexDocsCondition(1L).build());
                response = indicesAdmin().rolloverIndex(request).actionGet();
                assertThat(response.isRolledOver(), is(true));
                conditionStatus = response.getConditionStatus();
                assertThat(conditionStatus.size(), is(2));
                for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
                    if (entry.getKey().equals(new MaxDocsCondition(1L).toString())) {
                        assertThat(conditionStatus.get(new MaxDocsCondition(1L).toString()), is(true));
                    } else {
                        assertThat(conditionStatus.get(new OptimalShardCountCondition(2).toString()), is(true));
                    }
                }

                ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
                IndexMetadata thirdGenerationMeta = clusterStateAfterRollover.metadata()
                    .getProject()
                    .getIndexSafe(dataStream.getWriteIndex());

                assertThat(thirdGenerationMeta.getNumberOfShards(), is(2));
            } finally {
                // reset increase shards cooldown value
                updateClusterSettings(
                    Settings.builder().putNull(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.getKey())
                );

            }

        }

    }

    public void testLazyRolloverKeepsPreviousAutoshardingDecision() throws IOException {
        final String dataStreamName = "logs-es";

        putComposableIndexTemplate("my-template", List.of("logs-*"), indexSettings(3, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        indexDocs(dataStreamName, randomIntBetween(100, 200));

        {
            ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata().getProject().dataStreams().get(dataStreamName);

            Index firstGenerationIndex = clusterStateBeforeRollover.metadata()
                .getProject()
                .dataStreams()
                .get(dataStreamName)
                .getWriteIndex();
            IndexMetadata firstGenerationMeta = clusterStateBeforeRollover.getMetadata().getProject().index(firstGenerationIndex);

            List<ShardStats> shards = new ArrayList<>(firstGenerationMeta.getNumberOfShards());
            String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                .index(dataStreamBeforeRollover.getWriteIndex())
                .shard(0)
                .primaryShard()
                .currentNodeId();
            for (int i = 0; i < firstGenerationMeta.getNumberOfShards(); i++) {
                // the shard stats will yield a write load of 75.0 which will make the auto sharding service recommend an optimal number
                // of 5 shards
                shards.add(
                    getShardStats(
                        firstGenerationMeta,
                        i,
                        (long) Math.ceil(75.0 / firstGenerationMeta.getNumberOfShards()),
                        assignedShardNodeId
                    )
                );
            }

            mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, firstGenerationMeta, shards);
            assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

            ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
            DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
            IndexMetadata secondGenerationMeta = clusterStateAfterRollover.metadata().getProject().getIndexSafe(dataStream.getWriteIndex());

            // we auto sharded up to 5 shards
            assertThat(secondGenerationMeta.getNumberOfShards(), is(5));
        }

        {
            try {
                // eliminate the increase shards cooldown so there are no potential barriers to another increase shards option (we'll
                // actually also simulate the stats such that an increase to 7 is warranted) and execute a lazy rollover that should not
                // indeed auto shard up, but just keep the existing auto sharding event and create a new index with 5 shards (as dictated
                // by the existing auto sharding event)
                updateClusterSettings(
                    Settings.builder().put(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.getKey(), "0s")
                );

                ClusterState clusterStateBeforeRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStreamBeforeRollover = clusterStateBeforeRollover.getMetadata()
                    .getProject()
                    .dataStreams()
                    .get(dataStreamName);

                String assignedShardNodeId = clusterStateBeforeRollover.routingTable()
                    .index(dataStreamBeforeRollover.getWriteIndex())
                    .shard(0)
                    .primaryShard()
                    .currentNodeId();
                IndexMetadata secondGenIndex = clusterStateBeforeRollover.metadata()
                    .getProject()
                    .index(dataStreamBeforeRollover.getIndices().get(1));
                List<ShardStats> shards = new ArrayList<>(secondGenIndex.getNumberOfShards());
                for (int i = 0; i < secondGenIndex.getNumberOfShards(); i++) {
                    // the shard stats will yield a write load of 100.0 which will make the auto sharding service recommend an optimal
                    // number of 7 shards
                    shards.add(
                        getShardStats(secondGenIndex, i, (long) Math.ceil(100.0 / secondGenIndex.getNumberOfShards()), assignedShardNodeId)
                    );
                }

                mockStatsForIndex(clusterStateBeforeRollover, assignedShardNodeId, secondGenIndex, shards);

                RolloverRequest request = new RolloverRequest(dataStreamName, null);
                request.lazy(true);
                assertAcked(indicesAdmin().rolloverIndex(request).actionGet());

                // index some docs so the rollover is executed
                indexDocs(dataStreamName, 10);
                ClusterState clusterStateAfterRollover = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                DataStream dataStream = clusterStateAfterRollover.getMetadata().getProject().dataStreams().get(dataStreamName);
                IndexMetadata thirdGenerationIndex = clusterStateAfterRollover.metadata()
                    .getProject()
                    .getIndexSafe(dataStream.getWriteIndex());

                // we kept the number of shards to 5 as we did a lazy rollover
                assertThat(thirdGenerationIndex.getNumberOfShards(), is(5));
            } finally {
                // reset increase shards cooldown value
                updateClusterSettings(
                    Settings.builder().putNull(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.getKey())
                );
            }
        }
    }

    private static ShardStats getShardStats(IndexMetadata indexMeta, int shardIndex, long targetWriteLoad, String assignedShardNodeId) {
        ShardId shardId = new ShardId(indexMeta.getIndex(), shardIndex);
        Path path = createTempDir().resolve("indices").resolve(indexMeta.getIndexUUID()).resolve(String.valueOf(shardIndex));
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
        stats.docs = new DocsStats(100, 0, randomByteSizeValue().getBytes());
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats(
            new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, targetWriteLoad)
        );
        return new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0);
    }

    static void putComposableIndexTemplate(String id, List<String> patterns, @Nullable Settings settings) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settings))
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

    private static void mockStatsForIndex(
        ClusterState clusterState,
        String assignedShardNodeId,
        IndexMetadata indexMetadata,
        List<ShardStats> shards
    ) {
        for (DiscoveryNode node : clusterState.nodes().getAllNodes()) {
            // one node returns the stats for all our shards, the other nodes don't return any stats
            if (node.getId().equals(assignedShardNodeId)) {
                MockTransportService.getInstance(node.getName())
                    .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                        TransportIndicesStatsAction instance = internalCluster().getInstance(
                            TransportIndicesStatsAction.class,
                            node.getName()
                        );
                        channel.sendResponse(instance.new NodeResponse(node.getId(), indexMetadata.getNumberOfShards(), shards, List.of()));
                    });
            } else {
                MockTransportService.getInstance(node.getName())
                    .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                        TransportIndicesStatsAction instance = internalCluster().getInstance(
                            TransportIndicesStatsAction.class,
                            node.getName()
                        );
                        channel.sendResponse(instance.new NodeResponse(node.getId(), 0, List.of(), List.of()));
                    });
            }
        }
    }

    private static void resetTelemetry() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();
            telemetryPlugin.resetMeter();
        }
    }

    private static void assertTelemetry(String expectedEmittedMetric) {
        Map<String, List<Measurement>> measurements = new HashMap<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();

            telemetryPlugin.collect();

            List<String> autoShardingMetrics = telemetryPlugin.getRegisteredMetrics(InstrumentType.LONG_COUNTER)
                .stream()
                .filter(metric -> metric.startsWith("es.auto_sharding."))
                .sorted()
                .toList();

            assertEquals(autoShardingMetrics, MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.values().stream().sorted().toList());

            for (String metricName : MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.values()) {
                measurements.computeIfAbsent(metricName, n -> new ArrayList<>())
                    .addAll(telemetryPlugin.getLongCounterMeasurement(metricName));
            }
        }

        // assert other metrics not emitted
        MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.values()
            .stream()
            .filter(metric -> metric.equals(expectedEmittedMetric) == false)
            .forEach(metric -> assertThat(measurements.get(metric), empty()));

        assertThat(measurements.get(expectedEmittedMetric), hasSize(1));
        Measurement measurement = measurements.get(expectedEmittedMetric).get(0);
        assertThat(measurement.getLong(), is(1L));
        assertFalse(measurement.isDouble());
    }
}
