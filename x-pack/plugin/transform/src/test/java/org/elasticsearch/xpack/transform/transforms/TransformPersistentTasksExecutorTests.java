/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformPersistentTasksExecutorTests extends ESTestCase {

    public void testNodeVersionAssignment() {
        DiscoveryNodes.Builder nodes = buildNodes(false, true, true, true, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        assertThat(
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, true),
                cs.nodes().getAllNodes(), cs).getExecutorNode(),
            equalTo("current-data-node-with-1-tasks")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
                cs.nodes().getAllNodes(), cs).getExecutorNode(),
            equalTo("current-data-node-with-0-tasks-transform-remote-disabled")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-old-task-id", Version.V_7_5_0, null, true),
                cs.nodes().getAllNodes(), cs).getExecutorNode(),
            equalTo("past-data-node-1")
        );
    }

    public void testNodeAssignmentProblems() {
        // no data nodes
        DiscoveryNodes.Builder nodes = buildNodes(false, false, false, false, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        Assignment assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo("Not starting transform [new-task-id], reasons [current-data-node-with-transform-disabled:not a transform node]")
        );

        // dedicated transform node
        nodes = buildNodes(true, false, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("dedicated-transform-node"));

        // only an old node
        nodes = buildNodes(false, true, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_7_0, null, false),
            cs.nodes().getAllNodes(), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-transform-disabled:not a transform node"
                    + "|"
                    + "past-data-node-1:node has version: 7.5.0 but transform requires at least 7.7.0"
                    + "]"
            )
        );

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, false),
            cs.nodes().getAllNodes(), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));

        // no remote
        nodes = buildNodes(false, false, false, true, false);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(), cs);
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

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("current-data-node-with-0-tasks-transform-remote-disabled"));

        // no remote and disabled
        nodes = buildNodes(false, false, false, true, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(), cs);
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-0-tasks-transform-remote-disabled:"
                    + "transform requires a remote connection but remote is disabled"
                    + "|"
                    + "current-data-node-with-transform-disabled:not a transform node"
                    + "]"
            )
        );
        // old node, we do not know if remote is enabled
        nodes = buildNodes(false, true, false, true, false);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(), cs);
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));
    }

    public void testDoNotSelectOldNodes() {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);
        PersistentTasksCustomMetadata.Builder pTasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "transform-task-1",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", Version.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-1-task", "")
            );

        PersistentTasksCustomMetadata pTasks = pTasksBuilder.build();

        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, pTasks);

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "old-data-node-1",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)),
                    Version.V_7_2_0
                )
            )
            .add(
                new DiscoveryNode(
                    "current-data-node-with-1-task",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(
                        Arrays.asList(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE,
                            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                            Transform.TRANSFORM_ROLE
                        )
                    ),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "non-data-node-1",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
                    Version.CURRENT
                )
            );

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).nodes(nodes);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        Client client = mock(Client.class);
        TransformAuditor mockAuditor = mock(TransformAuditor.class);

        IndexBasedTransformConfigManager transformsConfigManager = new IndexBasedTransformConfigManager(client, xContentRegistry());
        TransformCheckpointService transformCheckpointService = new TransformCheckpointService(
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
        when(clusterService.state()).thenReturn(TransformInternalIndexTests.randomTransformClusterState());
        TransformPersistentTasksExecutor executor = new TransformPersistentTasksExecutor(
            client,
            transformServices,
            mock(ThreadPool.class),
            clusterService,
            Settings.EMPTY,
            TestIndexNameExpressionResolver.newInstance()
        );

        // old-data-node-1 prevents assignment
        assertNull(executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
                cs.nodes().getAllNodes(), cs).getExecutorNode());

        // remove the old 7.2 node
        nodes = DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "current-data-node-with-1-task",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, Transform.TRANSFORM_ROLE)),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "non-data-node-1",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
                    Version.CURRENT
                )
            );
        cs = ClusterState.builder(cs).nodes(nodes).build();

        assertThat(
            executor.getAssignment(new TransformTaskParams("new-old-task-id", Version.V_7_2_0, null, false),
                    cs.nodes().getAllNodes(), cs).getExecutorNode(),
            equalTo("current-data-node-with-1-task")
        );
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        assertEquals(
            0,
            TransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(cs, TestIndexNameExpressionResolver.newInstance()).size()
        );

        metadata = new Metadata.Builder(cs.metadata());
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

        csBuilder = ClusterState.builder(cs);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);
        List<String> result = TransformPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(
            csBuilder.build(),
            TestIndexNameExpressionResolver.newInstance()
        );
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    private void addIndices(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(TransformInternalIndexConstants.AUDIT_INDEX);
        indices.add(TransformInternalIndexConstants.LATEST_INDEX_NAME);
        for (String indexName : indices) {
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
            indexMetadata.settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            metadata.put(indexMetadata);
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
        boolean transformLocalOnlyNodes,
        boolean currentDataNode
    ) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();

        if (dedicatedTransformNode) {
            nodes.add(
                new DiscoveryNode(
                    "dedicated-transform-node",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(
                        Arrays.asList(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, Transform.TRANSFORM_ROLE)
                    ),
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
                    new HashSet<>(
                        Arrays.asList(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE,
                            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                            Transform.TRANSFORM_ROLE
                        )
                    ),
                    Version.V_7_5_0
                )
            );
        }

        if (transformRemoteNodes) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-2-tasks",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(
                        Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, Transform.TRANSFORM_ROLE)
                    ),
                    Version.CURRENT
                )
            )
                .add(
                    new DiscoveryNode(
                        "current-data-node-with-1-tasks",
                        buildNewFakeTransportAddress(),
                        Collections.emptyMap(),
                        new HashSet<>(
                            Arrays.asList(
                                DiscoveryNodeRole.MASTER_ROLE,
                                DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                                Transform.TRANSFORM_ROLE
                            )
                        ),
                        Version.CURRENT
                    )
                );
        }

        if (transformLocalOnlyNodes) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-0-tasks-transform-remote-disabled",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, Transform.TRANSFORM_ROLE)),
                    Version.CURRENT
                )
            );
        }

        if (currentDataNode) {
            nodes.add(
                new DiscoveryNode(
                    "current-data-node-with-transform-disabled",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(
                        Arrays.asList(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE,
                            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE
                        )
                    ),
                    Version.CURRENT
                )
            );
        }

        return nodes;
    }

    private ClusterState buildClusterState(DiscoveryNodes.Builder nodes) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);
        PersistentTasksCustomMetadata.Builder pTasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "transform-task-1",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", Version.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-1-tasks", "")
            )
            .addTask(
                "transform-task-2",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-2", Version.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-2-tasks", "")
            )
            .addTask(
                "transform-task-3",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-3", Version.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-2-tasks", "")
            );

        PersistentTasksCustomMetadata pTasks = pTasksBuilder.build();
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, pTasks);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).nodes(nodes);
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        return csBuilder.build();

    }

    public TransformPersistentTasksExecutor buildTaskExecutor() {
        Client client = mock(Client.class);
        TransformAuditor mockAuditor = mock(TransformAuditor.class);
        IndexBasedTransformConfigManager transformsConfigManager = new IndexBasedTransformConfigManager(client, xContentRegistry());
        TransformCheckpointService transformCheckpointService = new TransformCheckpointService(
            Clock.systemUTC(),
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
        when(clusterService.state()).thenReturn(TransformInternalIndexTests.randomTransformClusterState());

        return new TransformPersistentTasksExecutor(
            client,
            transformServices,
            mock(ThreadPool.class),
            clusterService,
            Settings.EMPTY,
            TestIndexNameExpressionResolver.newInstance()
        );
    }
}
