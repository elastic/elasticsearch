/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class TransportStartDataFrameAnalyticsActionTests extends ESTestCase {

    public void testVerifyIndicesPrimaryShardsAreActive() {

        // At present the only critical index is the config index
        String indexName = AnomalyDetectorsIndex.configIndexName();

        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();

        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
        indexMetaData.settings(Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        if (indexName.equals(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX)) {
            indexMetaData.putAlias(new AliasMetaData.Builder(AnomalyDetectorsIndex.jobStateIndexWriteAlias()));
        }
        metaData.put(indexMetaData);
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
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertThat(TransportStartDataFrameAnalyticsAction.verifyIndicesPrimaryShardsAreActive(cs, indexName), empty());

        metaData = new MetaData.Builder(cs.metaData());
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
        csBuilder.metaData(metaData);
        List<String> result = TransportStartDataFrameAnalyticsAction.verifyIndicesPrimaryShardsAreActive(csBuilder.build(), indexName);
        assertThat(result, contains(indexName));
    }
}
