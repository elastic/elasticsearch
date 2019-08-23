/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameTransformPersistentTasksExecutorTests extends ESTestCase {

    public void testNodeVersionAssignment() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);
        PersistentTasksCustomMetaData.Builder pTasksBuilder = PersistentTasksCustomMetaData.builder()
            .addTask("data-frame-task-1",
                DataFrameTransform.NAME,
                new DataFrameTransform("data-frame-task-1", Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-1-tasks", ""))
            .addTask("data-frame-task-2",
                DataFrameTransform.NAME,
                new DataFrameTransform("data-frame-task-2", Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-2-tasks", ""))
            .addTask("data-frame-task-3",
                DataFrameTransform.NAME,
                new DataFrameTransform("data-frame-task-3", Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-2-tasks", ""));

        PersistentTasksCustomMetaData pTasks = pTasksBuilder.build();

        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, pTasks);

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("past-data-node-1",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE),
                Version.V_7_2_0))
            .add(new DiscoveryNode("current-data-node-with-2-tasks",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT))
            .add(new DiscoveryNode("non-data-node-1",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT))
            .add(new DiscoveryNode("current-data-node-with-1-tasks",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT));

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
            .nodes(nodes);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        Client client = mock(Client.class);
        DataFrameAuditor mockAuditor = mock(DataFrameAuditor.class);
        DataFrameTransformsConfigManager transformsConfigManager = new DataFrameTransformsConfigManager(client, xContentRegistry());
        DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService = new DataFrameTransformsCheckpointService(client,
            transformsConfigManager, mockAuditor);
        ClusterSettings cSettings = new ClusterSettings(Settings.EMPTY,
            Collections.singleton(DataFrameTransformTask.NUM_FAILURE_RETRIES_SETTING));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
        DataFrameTransformPersistentTasksExecutor executor = new DataFrameTransformPersistentTasksExecutor(client,
            transformsConfigManager,
            dataFrameTransformsCheckpointService, mock(SchedulerEngine.class),
            new DataFrameAuditor(client, ""),
            mock(ThreadPool.class),
            clusterService,
            Settings.EMPTY);

        assertThat(executor.getAssignment(new DataFrameTransform("new-task-id", Version.CURRENT, null), cs).getExecutorNode(),
            equalTo("current-data-node-with-1-tasks"));
        assertThat(executor.getAssignment(new DataFrameTransform("new-old-task-id", Version.V_7_2_0, null), cs).getExecutorNode(),
            equalTo("past-data-node-1"));
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertEquals(0, DataFrameTransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(cs).size());

        metaData = new MetaData.Builder(cs.metaData());
        routingTable = new RoutingTable.Builder(cs.routingTable());
        String indexToRemove = DataFrameInternalIndex.LATEST_INDEX_NAME;
        if (randomBoolean()) {
            routingTable.remove(indexToRemove);
        } else {
            Index index = new Index(indexToRemove, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);
        List<String> result = DataFrameTransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(csBuilder.build());
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    private void addIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(DataFrameInternalIndex.AUDIT_INDEX);
        indices.add(DataFrameInternalIndex.LATEST_INDEX_NAME);
        for (String indexName : indices) {
            IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
            indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            metaData.put(indexMetaData);
            Index index = new Index(indexName, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            shardRouting = shardRouting.moveToStarted();
            routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }
    }

}
