/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
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
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndexTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformPersistentTasksExecutorTests extends ESTestCase {

    public void testNodeVersionAssignment() {
        DiscoveryNodes.Builder nodes = buildNodes(false, true, true, true, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        assertThat(
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, true), cs).getExecutorNode(),
            equalTo("current-data-node-with-1-tasks")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false), cs).getExecutorNode(),
            equalTo("current-data-node-with-0-tasks-transform-remote-disabled")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-old-task-id", Version.V_7_5_0, null, true), cs).getExecutorNode(),
            equalTo("past-data-node-1")
        );
    }

    public void testNodeAssignmentProblems() {
        // no data nodes
        DiscoveryNodes.Builder nodes = buildNodes(false, false, false, false, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        Assignment assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo("Not starting transform [new-task-id], reasons [current-data-node-with-transform-disabled:transform not enabled]")
        );

        // dedicated transform node
        nodes = buildNodes(true, false, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("dedicated-transform-node"));

        // only an old node
        nodes = buildNodes(false, true, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_7_0, null, false), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-transform-disabled:transform not enabled"
                    + "|"
                    + "past-data-node-1:node has version: 7.5.0 but transform requires at least 7.7.0"
                    + "]"
            )
        );

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, false), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));

        // no remote
        nodes = buildNodes(false, false, false, true, false);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-0-tasks-transform-remote-disabled:"
                    + "transform requires a remote connection but remote is disabled"
                    + "]"
            )
        );

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("current-data-node-with-0-tasks-transform-remote-disabled"));

        // no remote and disabled
        nodes = buildNodes(false, false, false, true, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-0-tasks-transform-remote-disabled:"
                    + "transform requires a remote connection but remote is disabled"
                    + "|"
                    + "current-data-node-with-transform-disabled:transform not enabled"
                    + "]"
            )
        );
        // old node, we do not know if remote is enabled
        nodes = buildNodes(false, true, false, true, false);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertEquals(0, TransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(cs, new IndexNameExpressionResolver()).size());

        metaData = new MetaData.Builder(cs.metaData());
        routingTable = new RoutingTable.Builder(cs.routingTable());
        String indexToRemove = TransformInternalIndexConstants.LATEST_INDEX_NAME;
        if (randomBoolean()) {
            routingTable.remove(indexToRemove);
        } else {
            Index index = new Index(indexToRemove, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                shardId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
            );
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(
                IndexRoutingTable.builder(index).addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build())
            );
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);
        List<String> result = TransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(
            csBuilder.build(),
            new IndexNameExpressionResolver()
        );
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    private void addIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(TransformInternalIndexConstants.AUDIT_INDEX);
        indices.add(TransformInternalIndexConstants.LATEST_INDEX_NAME);
        for (String indexName : indices) {
            IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
            indexMetaData.settings(
                Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            metaData.put(indexMetaData);
            Index index = new Index(indexName, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                shardId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
            );
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            shardRouting = shardRouting.moveToStarted();
            routingTable.add(
                IndexRoutingTable.builder(index).addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build())
            );
        }
    }

    private DiscoveryNodes.Builder buildNodes(
        boolean dedicatedTransformNode,
        boolean pastDataNode,
        boolean transformRemoteNodes,
        boolean transformLocanOnlyNodes,
        boolean currentDataNode
    ) {

        Map<String, String> transformNodeAttributes = new HashMap<>();
        transformNodeAttributes.put(Transform.TRANSFORM_ENABLED_NODE_ATTR, "true");
        transformNodeAttributes.put(Transform.TRANSFORM_REMOTE_ENABLED_NODE_ATTR, "true");
        Map<String, String> transformNodeAttributesDisabled = new HashMap<>();
        transformNodeAttributesDisabled.put(Transform.TRANSFORM_ENABLED_NODE_ATTR, "false");
        transformNodeAttributesDisabled.put(Transform.TRANSFORM_REMOTE_ENABLED_NODE_ATTR, "true");
        Map<String, String> transformNodeAttributesNoRemote = new HashMap<>();
        transformNodeAttributesNoRemote.put(Transform.TRANSFORM_ENABLED_NODE_ATTR, "true");
        transformNodeAttributesNoRemote.put(Transform.TRANSFORM_REMOTE_ENABLED_NODE_ATTR, "false");

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();

        if (dedicatedTransformNode) {
            nodes.add(
                new DiscoveryNode(
                    "dedicated-transform-node",
                    buildNewFakeTransportAddress(),
                    transformNodeAttributes,
                    Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
                    Version.CURRENT
                )
            );
        }

        if (pastDataNode) {
            nodes.add(
                new DiscoveryNode(
                    "past-data-node-1",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)),
                    Version.V_7_5_0
                )
            );
        }

        if (transformRemoteNodes) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-2-tasks",
                    buildNewFakeTransportAddress(),
                    transformNodeAttributes,
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE)),
                    Version.CURRENT
                )
            )
                .add(
                    new DiscoveryNode(
                        "current-data-node-with-1-tasks",
                        buildNewFakeTransportAddress(),
                        transformNodeAttributes,
                        new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE)),
                        Version.CURRENT
                    )
                );
        }

        if (transformLocanOnlyNodes) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-0-tasks-transform-remote-disabled",
                    buildNewFakeTransportAddress(),
                    transformNodeAttributesNoRemote,
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)),
                    Version.CURRENT
                )
            );
        }

        if (currentDataNode) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-transform-disabled",
                    buildNewFakeTransportAddress(),
                    transformNodeAttributesDisabled,
                    Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE),
                    Version.CURRENT
                )
            );
        }

        return nodes;
    }

    private ClusterState buildClusterState(DiscoveryNodes.Builder nodes) {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);
        PersistentTasksCustomMetaData.Builder pTasksBuilder = PersistentTasksCustomMetaData.builder()
            .addTask(
                "transform-task-1",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", Version.CURRENT, null, false),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-1-tasks", "")
            )
            .addTask(
                "transform-task-2",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-2", Version.CURRENT, null, false),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-2-tasks", "")
            )
            .addTask(
                "transform-task-3",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-3", Version.CURRENT, null, false),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-2-tasks", "")
            );

        PersistentTasksCustomMetaData pTasks = pTasksBuilder.build();
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, pTasks);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).nodes(nodes);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        return csBuilder.build();

    }

    public TransformPersistentTasksExecutor buildTaskExecutor() {
        Client client = mock(Client.class);
        TransformAuditor mockAuditor = mock(TransformAuditor.class);
        IndexBasedTransformConfigManager transformsConfigManager = new IndexBasedTransformConfigManager(client, xContentRegistry());
        TransformCheckpointService transformCheckpointService = new TransformCheckpointService(
            client,
            Settings.EMPTY,
            new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null),
            transformsConfigManager,
            mockAuditor
        );
        TransformServices transformServices = new TransformServices(
            transformsConfigManager,
            transformCheckpointService,
            mockAuditor,
            mock(SchedulerEngine.class)
        );

        ClusterSettings cSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(Transform.NUM_FAILURE_RETRIES_SETTING));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
        when(clusterService.state()).thenReturn(TransformInternalIndexTests.STATE_WITH_LATEST_VERSIONED_INDEX_TEMPLATE);

        return new TransformPersistentTasksExecutor(
            client,
            transformServices,
            mock(ThreadPool.class),
            clusterService,
            Settings.EMPTY,
            new IndexNameExpressionResolver()
        );
    }
}
