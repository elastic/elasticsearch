/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.task;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
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
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive;

public class AbstractJobPersistentTasksExecutorTests extends ESTestCase {

    public void testVerifyIndicesPrimaryShardsAreActive() {
        final IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        assertEquals(
            0,
            verifyIndicesPrimaryShardsAreActive(
                cs,
                resolver,
                true,
                ".ml-anomalies-shared",
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                MlMetaIndex.indexName(),
                MlConfigIndex.indexName()
            ).size()
        );

        metadata = Metadata.builder(cs.metadata());
        routingTable = new RoutingTable.Builder(cs.routingTable());
        String indexToRemove = randomFrom(
            resolver.concreteIndexNames(
                cs,
                IndicesOptions.lenientExpandOpen(),
                ".ml-anomalies-shared",
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                MlMetaIndex.indexName(),
                MlConfigIndex.indexName()
            )
        );
        if (randomBoolean()) {
            routingTable.remove(indexToRemove);
        } else {
            Index index = new Index(indexToRemove, "_uuid");
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

        csBuilder = ClusterState.builder(cs);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);
        List<String> result = verifyIndicesPrimaryShardsAreActive(
            csBuilder.build(),
            resolver,
            true,
            ".ml-anomalies-shared",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            MlMetaIndex.indexName(),
            MlConfigIndex.indexName()
        );
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    private void addIndices(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(MlConfigIndex.indexName());
        indices.add(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        indices.add(MlMetaIndex.indexName());
        indices.add(NotificationsIndex.NOTIFICATIONS_INDEX);
        indices.add(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        for (String indexName : indices) {
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
            indexMetadata.settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            if (indexName.equals(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX)) {
                indexMetadata.putAlias(new AliasMetadata.Builder(AnomalyDetectorsIndex.jobStateIndexWriteAlias()));
            }
            metadata.put(indexMetadata);
            Index index = new Index(indexName, "_uuid");
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
            routingTable.add(
                IndexRoutingTable.builder(index).addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting))
            );
        }
    }

}
