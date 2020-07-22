/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.xpack.core.ml.MlConfigIndex;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class TransportStartDataFrameAnalyticsActionTests extends ESTestCase {

    public void testVerifyIndicesPrimaryShardsAreActive() {

        // At present the only critical index is the config index
        String indexName = MlConfigIndex.indexName();

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();

        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metadata.put(indexMetadata);
        Index index = new Index(indexName, "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        assertThat(
            TransportStartDataFrameAnalyticsAction.verifyIndicesPrimaryShardsAreActive(cs, new IndexNameExpressionResolver(), indexName),
            empty());

        metadata = new Metadata.Builder(cs.metadata());
        routingTable = new RoutingTable.Builder(cs.routingTable());
        if (randomBoolean()) {
            routingTable.remove(indexName);
        } else {
            index = new Index(indexName, "_uuid");
            shardId = new ShardId(index, 0);
            shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);
        List<String> result = TransportStartDataFrameAnalyticsAction.verifyIndicesPrimaryShardsAreActive(csBuilder.build(),
            new IndexNameExpressionResolver(), indexName);
        assertThat(result, contains(indexName));
    }
}
