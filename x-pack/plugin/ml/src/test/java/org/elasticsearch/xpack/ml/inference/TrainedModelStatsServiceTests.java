/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TrainedModelStatsServiceTests extends ESTestCase {

    public void testVerifyIndicesExistAndPrimaryShardsAreActive() {
        String aliasName = MlStatsIndex.writeAlias();
        String concreteIndex = ".ml-stats-000001";
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

        {
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();

            // With no index
            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);

            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver), equalTo(false));
        }
        {
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            // With concrete ONLY
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            metadata.put(indexMetadata);
            addToRoutingTable(concreteIndex, routingTable);

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver), equalTo(false));
        }
        {
            // With Alias And Concrete index
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            metadata.put(indexMetadata);
            addToRoutingTable(concreteIndex, routingTable);

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver), equalTo(true));
        }
        {
            // With Alias And Concrete index but routing is missing or concrete index
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            metadata.put(indexMetadata);
            addToRoutingTable(concreteIndex, routingTable);
            if (randomBoolean()) {
                routingTable.remove(concreteIndex);
            } else {
                Index index = new Index(concreteIndex, "_uuid");
                ShardId shardId = new ShardId(index, 0);
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                    ShardRouting.Role.DEFAULT
                );
                shardRouting = shardRouting.initialize("node_id", null, 0L);
                routingTable.add(
                    IndexRoutingTable.builder(index).addIndexShard(IndexShardRoutingTable.builder(shardId).addShard(shardRouting))
                );
            }

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver), equalTo(false));
        }
    }

    public void testUpdateStatsUpgradeMode() {
        String aliasName = MlStatsIndex.writeAlias();
        String concreteIndex = ".ml-stats-000001";
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

        // create a valid index routing so persistence will occur
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        addToRoutingTable(concreteIndex, routingTableBuilder);
        RoutingTable routingTable = routingTableBuilder.build();

        // cannot mock OriginSettingClient as it is final so mock the client
        Client client = mock(Client.class);
        OriginSettingClient originSettingClient = new OriginSettingClient(client, "modelstatsservicetests");
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ResultsPersisterService persisterService = mock(ResultsPersisterService.class);

        TrainedModelStatsService service = new TrainedModelStatsService(
            persisterService,
            originSettingClient,
            resolver,
            clusterService,
            threadPool
        );

        InferenceStats.Accumulator accumulator = new InferenceStats.Accumulator("testUpdateStatsUpgradeMode", "test-node", 1L);

        {
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            Metadata.Builder metadata = Metadata.builder().put(indexMetadata);

            ClusterState clusterState = ClusterState.builder(new ClusterName("upgrade-mode-test-initial-state"))
                .routingTable(routingTable)
                .metadata(metadata)
                .build();
            ClusterChangedEvent change = new ClusterChangedEvent("created-from-test", clusterState, clusterState);

            service.setClusterState(change);

            // queue some stats to be persisted
            service.queueStats(accumulator.currentStats(Instant.now()), false);

            service.updateStats();
            verify(persisterService, times(1)).bulkIndexWithRetry(any(), any(), any(), any());
        }
        {
            // test with upgrade mode turned on

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );

            // now set the upgrade mode
            Metadata.Builder metadata = Metadata.builder()
                .put(indexMetadata)
                .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(true).build());

            ClusterState clusterState = ClusterState.builder(new ClusterName("upgrade-mode-test-upgrade-enabled"))
                .routingTable(routingTable)
                .metadata(metadata)
                .build();
            ClusterChangedEvent change = new ClusterChangedEvent("created-from-test", clusterState, clusterState);

            service.setClusterState(change);

            // queue some stats to be persisted
            service.queueStats(accumulator.currentStats(Instant.now()), false);

            service.updateStats();
            verify(persisterService, times(1)).bulkIndexWithRetry(any(), any(), any(), any());
        }
        {
            // This time turn off upgrade mode

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );

            Metadata.Builder metadata = Metadata.builder()
                .put(indexMetadata)
                .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(false).build());

            ClusterState clusterState = ClusterState.builder(new ClusterName("upgrade-mode-test-upgrade-disabled"))
                .routingTable(routingTable)
                .metadata(metadata)
                .build();

            ClusterChangedEvent change = new ClusterChangedEvent("created-from-test", clusterState, clusterState);

            service.setClusterState(change);
            service.updateStats();
            verify(persisterService, times(2)).bulkIndexWithRetry(any(), any(), any(), any());
        }
    }

    public void testUpdateStatsResetMode() {
        String aliasName = MlStatsIndex.writeAlias();
        String concreteIndex = ".ml-stats-000001";
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

        // create a valid index routing so persistence will occur
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        addToRoutingTable(concreteIndex, routingTableBuilder);
        RoutingTable routingTable = routingTableBuilder.build();

        // cannot mock OriginSettingClient as it is final so mock the client
        Client client = mock(Client.class);
        OriginSettingClient originSettingClient = new OriginSettingClient(client, "modelstatsservicetests");
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ResultsPersisterService persisterService = mock(ResultsPersisterService.class);

        TrainedModelStatsService service = new TrainedModelStatsService(
            persisterService,
            originSettingClient,
            resolver,
            clusterService,
            threadPool
        );

        InferenceStats.Accumulator accumulator = new InferenceStats.Accumulator("testUpdateStatsUpgradeMode", "test-node", 1L);

        {
            // test with reset mode turned on

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );

            // now set the upgrade mode
            Metadata.Builder metadata = Metadata.builder()
                .put(indexMetadata)
                .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(true).build());

            ClusterState clusterState = ClusterState.builder(new ClusterName("upgrade-mode-test-upgrade-enabled"))
                .routingTable(routingTable)
                .metadata(metadata)
                .build();
            ClusterChangedEvent change = new ClusterChangedEvent("created-from-test", clusterState, clusterState);

            service.setClusterState(change);

            // queue some stats to be persisted
            service.queueStats(accumulator.currentStats(Instant.now()), false);

            service.updateStats();
            verify(persisterService, times(0)).bulkIndexWithRetry(any(), any(), any(), any());
        }
        {
            // This time turn off reset mode

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );

            Metadata.Builder metadata = Metadata.builder()
                .put(indexMetadata)
                .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(false).build());

            ClusterState clusterState = ClusterState.builder(new ClusterName("upgrade-mode-test-upgrade-disabled"))
                .routingTable(routingTable)
                .metadata(metadata)
                .build();

            ClusterChangedEvent change = new ClusterChangedEvent("created-from-test", clusterState, clusterState);

            service.setClusterState(change);
            service.updateStats();
            verify(persisterService, times(1)).bulkIndexWithRetry(any(), any(), any(), any());
        }
    }

    private static void addToRoutingTable(String concreteIndex, RoutingTable.Builder routingTable) {
        Index index = new Index(concreteIndex, "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        routingTable.add(IndexRoutingTable.builder(index).addIndexShard(IndexShardRoutingTable.builder(shardId).addShard(shardRouting)));
    }
}
