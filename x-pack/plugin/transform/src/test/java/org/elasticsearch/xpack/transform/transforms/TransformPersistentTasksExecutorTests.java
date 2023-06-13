/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndexTests;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, true), cs.nodes().getAllNodes(), cs)
                .getExecutorNode(),
            equalTo("current-data-node-with-1-tasks")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-task-id", Version.CURRENT, null, false), cs.nodes().getAllNodes(), cs)
                .getExecutorNode(),
            equalTo("current-data-node-with-0-tasks-transform-remote-disabled")
        );
        assertThat(
            executor.getAssignment(new TransformTaskParams("new-old-task-id", Version.V_7_7_0, null, true), cs.nodes().getAllNodes(), cs)
                .getExecutorNode(),
            equalTo("past-data-node-1")
        );
    }

    public void testNodeAssignmentProblems() {
        // no data nodes
        DiscoveryNodes.Builder nodes = buildNodes(false, false, false, false, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        Assignment assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo("Not starting transform [new-task-id], reasons [current-data-node-with-transform-disabled:not a transform node]")
        );

        // dedicated transform node
        nodes = buildNodes(true, false, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("dedicated-transform-node"));

        // only an old node
        nodes = buildNodes(false, true, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.V_8_0_0, null, false),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-transform-disabled:not a transform node"
                    + "|"
                    + "past-data-node-1:node has version: 7.7.0 but transform requires at least 8.0.0"
                    + "]"
            )
        );

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.V_7_5_0, null, false),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));

        // no remote
        nodes = buildNodes(false, false, false, true, false);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(),
            cs
        );
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

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.CURRENT, null, false),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("current-data-node-with-0-tasks-transform-remote-disabled"));

        // no remote and disabled
        nodes = buildNodes(false, false, false, true, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(),
            cs
        );
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

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", Version.V_7_5_0, null, true),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNotNull(assignment.getExecutorNode());
        assertThat(assignment.getExecutorNode(), equalTo("past-data-node-1"));
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

        metadata = Metadata.builder(cs.metadata());
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
            indexMetadata.settings(indexSettings(Version.CURRENT, 1, 0));
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
                DiscoveryNodeUtils.builder("dedicated-transform-node")
                    .roles(
                        Set.of(
                            DiscoveryNodeRole.MASTER_ROLE,
                            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                            DiscoveryNodeRole.TRANSFORM_ROLE
                        )
                    )
                    .build()
            );
        }

        if (pastDataNode) {
            nodes.add(
                DiscoveryNodeUtils.builder("past-data-node-1")
                    .roles(
                        Set.of(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE,
                            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                            DiscoveryNodeRole.TRANSFORM_ROLE
                        )
                    )
                    .version(Version.V_7_7_0)
                    .build()
            );
        }

        if (transformRemoteNodes) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-2-tasks")
                    .roles(
                        Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, DiscoveryNodeRole.TRANSFORM_ROLE)
                    )
                    .build()
            )
                .add(
                    DiscoveryNodeUtils.builder("current-data-node-with-1-tasks")
                        .roles(
                            Set.of(
                                DiscoveryNodeRole.MASTER_ROLE,
                                DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                                DiscoveryNodeRole.TRANSFORM_ROLE
                            )
                        )
                        .build()
                );
        }

        if (transformLocalOnlyNodes) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-0-tasks-transform-remote-disabled")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.TRANSFORM_ROLE))
                    .build()
            );
        }

        if (currentDataNode) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-transform-disabled")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                    .build()
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

    private TransformPersistentTasksExecutor buildTaskExecutor() {
        ClusterService clusterService = mock(ClusterService.class);
        Client client = mock(Client.class);
        TransformAuditor mockAuditor = mock(TransformAuditor.class);
        IndexBasedTransformConfigManager transformsConfigManager = new IndexBasedTransformConfigManager(
            clusterService,
            TestIndexNameExpressionResolver.newInstance(),
            client,
            xContentRegistry()
        );
        Clock clock = Clock.systemUTC();
        ThreadPool threadPool = mock(ThreadPool.class);
        TransformCheckpointService transformCheckpointService = new TransformCheckpointService(
            clock,
            Settings.EMPTY,
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                null,
                (TaskManager) null
            ),
            transformsConfigManager,
            mockAuditor
        );
        TransformServices transformServices = new TransformServices(
            transformsConfigManager,
            transformCheckpointService,
            mockAuditor,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY)
        );

        ClusterSettings cSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(Transform.NUM_FAILURE_RETRIES_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
        when(clusterService.state()).thenReturn(TransformInternalIndexTests.randomTransformClusterState());

        return new TransformPersistentTasksExecutor(
            client,
            transformServices,
            threadPool,
            clusterService,
            Settings.EMPTY,
            TestIndexNameExpressionResolver.newInstance()
        );
    }
}
