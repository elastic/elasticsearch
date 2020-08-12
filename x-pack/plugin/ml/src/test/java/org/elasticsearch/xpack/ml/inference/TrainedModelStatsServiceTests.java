/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.Version;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;

import static org.hamcrest.Matchers.equalTo;

public class TrainedModelStatsServiceTests extends ESTestCase {

    public void testVerifyIndicesExistAndPrimaryShardsAreActive() {
        String aliasName = MlStatsIndex.writeAlias();
        String concreteIndex = ".ml-stats-000001";
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();

        {
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();

            // With no index
            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);

            csBuilder.build();
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver),
                equalTo(false));
        }
        {
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            // With concrete ONLY
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .settings(Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            metadata.put(indexMetadata);
            addToRoutingTable(concreteIndex, routingTable);

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver),
                equalTo(false));
        }
        {
            // With Alias And Concrete index
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                );
            metadata.put(indexMetadata);
            addToRoutingTable(concreteIndex, routingTable);

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver),
                equalTo(true));
        }
        {
            // With Alias And Concrete index but routing is missing or concrete index
            Metadata.Builder metadata = Metadata.builder();
            RoutingTable.Builder routingTable = RoutingTable.builder();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(concreteIndex)
                .putAlias(AliasMetadata.builder(aliasName).isHidden(true).build())
                .settings(Settings.builder()
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
                ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
                shardRouting = shardRouting.initialize("node_id", null, 0L);
                routingTable.add(IndexRoutingTable.builder(index)
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
            }

            ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .routingTable(routingTable.build())
                .metadata(metadata);
            assertThat(TrainedModelStatsService.verifyIndicesExistAndPrimaryShardsAreActive(csBuilder.build(), resolver),
                equalTo(false));
        }
    }

    private static void addToRoutingTable(String concreteIndex, RoutingTable.Builder routingTable) {
        Index index = new Index(concreteIndex, "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
    }
}
