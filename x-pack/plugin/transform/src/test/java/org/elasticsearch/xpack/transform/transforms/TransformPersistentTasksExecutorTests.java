/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.DefaultTransformExtension;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndexTests;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransformPersistentTasksExecutorTests extends ESTestCase {
    private static ThreadPool threadPool;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(TransformPersistentTasksExecutorTests.class.getSimpleName()) {
            @Override
            public ExecutorService executor(String name) {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor name) {
                command.run();
                return null;
            }
        };
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testNodeVersionAssignment() {
        DiscoveryNodes.Builder nodes = buildNodes(false, true, true, true, true);
        ClusterState cs = buildClusterState(nodes);
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        assertThat(
            executor.getAssignment(
                new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, true),
                cs.nodes().getAllNodes(),
                cs
            ).getExecutorNode(),
            equalTo("current-data-node-with-1-tasks")
        );
        assertThat(
            executor.getAssignment(
                new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
                cs.nodes().getAllNodes(),
                cs
            ).getExecutorNode(),
            equalTo("current-data-node-with-0-tasks-transform-remote-disabled")
        );
        assertThat(
            executor.getAssignment(
                new TransformTaskParams("new-old-task-id", TransformConfigVersion.V_7_7_0, null, true),
                cs.nodes().getAllNodes(),
                cs
            ).getExecutorNode(),
            equalTo("past-data-node-1")
        );
    }

    public void testNodeAssignmentProblems() {
        // no nodes
        ClusterState cs = buildClusterState(DiscoveryNodes.builder());
        TransformPersistentTasksExecutor executor = buildTaskExecutor();

        Assignment assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
            List.of(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons [cluster-uuid:No Discovery Nodes found in cluster state."
                    + " Check cluster health and troubleshoot missing Discovery Nodes.]"
            )
        );

        // no data nodes, but the cluster state is empty
        DiscoveryNodes.Builder nodes = buildNodes(false, false, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
            List.of(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo("Not starting transform [new-task-id], reasons [current-data-node-with-transform-disabled:not a transform node]")
        );

        // no data nodes
        nodes = buildNodes(false, false, false, false, true);
        cs = buildClusterState(nodes);
        executor = buildTaskExecutor();

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
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
            new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
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
            new TransformTaskParams("new-task-id", TransformConfigVersion.V_8_0_0, null, false),
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
                    + "past-data-node-1:node supports transform config version: 7.7.0 but transform requires at least 8.0.0"
                    + "]"
            )
        );

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", TransformConfigVersion.V_7_5_0, null, false),
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
            new TransformTaskParams("new-task-id", TransformConfigVersion.V_7_5_0, null, true),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-0-tasks-transform-remote-disabled:"
                    + "transform requires a remote connection but the node does not have the remote_cluster_client role"
                    + "]"
            )
        );

        assignment = executor.getAssignment(
            new TransformTaskParams("new-task-id", TransformConfigVersion.CURRENT, null, false),
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
            new TransformTaskParams("new-task-id", TransformConfigVersion.V_7_5_0, null, true),
            cs.nodes().getAllNodes(),
            cs
        );
        assertNull(assignment.getExecutorNode());
        assertThat(
            assignment.getExplanation(),
            equalTo(
                "Not starting transform [new-task-id], reasons ["
                    + "current-data-node-with-0-tasks-transform-remote-disabled:"
                    + "transform requires a remote connection but the node does not have the remote_cluster_client role"
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
            new TransformTaskParams("new-task-id", TransformConfigVersion.V_7_5_0, null, true),
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

    public void testNodeOperation() {
        var transformsConfigManager = new InMemoryTransformConfigManager();
        var transformScheduler = new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO);
        var taskExecutor = buildTaskExecutor(transformServices(transformsConfigManager, transformScheduler));

        var transformId = "testNodeOperation";
        var params = mockTaskParams(transformId);

        putTransformConfiguration(transformsConfigManager, transformId);
        var task = mockTransformTask();
        taskExecutor.nodeOperation(task, params, mock());

        verify(task).start(isNull(), any());
    }

    private void putTransformConfiguration(TransformConfigManager configManager, String transformId) {
        configManager.putTransformConfiguration(
            TransformConfigTests.randomTransformConfig(transformId, TimeValue.timeValueMillis(1), TransformConfigVersion.CURRENT),
            ActionListener.<Boolean>noop().delegateResponse((l, e) -> fail(e))
        );
    }

    public void testNodeOperationStartupRetry() throws Exception {
        var failFirstCall = new AtomicBoolean(true);
        var transformsConfigManager = new InMemoryTransformConfigManager() {
            @Override
            public void getTransformConfiguration(String transformId, ActionListener<TransformConfig> resultListener) {
                if (failFirstCall.compareAndSet(true, false)) {
                    resultListener.onFailure(new IllegalStateException("Failing first call."));
                } else {
                    super.getTransformConfiguration(transformId, resultListener);
                }
            }
        };

        var transformScheduler = new TransformScheduler(Clock.systemUTC(), threadPool, fastRetry(), TimeValue.ZERO);
        var taskExecutor = buildTaskExecutor(transformServices(transformsConfigManager, transformScheduler));

        var transformId = "testNodeOperationStartupRetry";
        var params = mockTaskParams(transformId);
        putTransformConfiguration(transformsConfigManager, transformId);

        var task = mockTransformTask();
        taskExecutor.nodeOperation(task, params, mock());

        // skip waiting for the scheduler to run the task a second time and just rerun it now
        transformScheduler.scheduleNow(transformId);

        // verify the retry listener set the state to TransformTaskState.STARTED + IndexerState.STOPPED
        verify(task).persistStateToClusterState(argThat(state -> {
            assertThat(TransformTaskState.STARTED, equalTo(state.getTaskState()));
            assertThat(IndexerState.STOPPED, equalTo(state.getIndexerState()));
            return true;
        }), any());
        verify(task).start(isNull(), any());
    }

    private Settings fastRetry() {
        // must be >= [1s]
        return Settings.builder().put(Transform.SCHEDULER_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1)).build();
    }

    private TransformTaskParams mockTaskParams(String transformId) {
        var params = mock(TransformTaskParams.class);
        when(params.getId()).thenReturn(transformId);
        when(params.getFrequency()).thenReturn(TimeValue.timeValueSeconds(1));
        return params;
    }

    private TransformTask mockTransformTask() {
        var task = mock(TransformTask.class);
        when(task.setAuthState(any(AuthorizationState.class))).thenReturn(task);
        when(task.setNumFailureRetries(anyInt())).thenReturn(task);
        when(task.getParentTaskId()).thenReturn(TaskId.EMPTY_TASK_ID);
        when(task.getContext()).thenReturn(mock());
        doAnswer(a -> fail(a.getArgument(0, Throwable.class))).when(task).fail(any(Throwable.class), any(String.class), any());
        when(task.getState()).thenReturn(
            new TransformState(TransformTaskState.STOPPED, IndexerState.STOPPED, null, 0, null, null, null, false, null)
        );
        return task;
    }

    private void addIndices(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(TransformInternalIndexConstants.AUDIT_INDEX);
        indices.add(TransformInternalIndexConstants.LATEST_INDEX_NAME);
        for (String indexName : indices) {
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
            indexMetadata.settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, "_uuid"));
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
                    .attributes(
                        Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.CURRENT.toString())
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
                    .attributes(
                        Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.V_7_7_0.toString())
                    )
                    .build()
            );
        }

        if (transformRemoteNodes) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-2-tasks")
                    .roles(
                        Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, DiscoveryNodeRole.TRANSFORM_ROLE)
                    )
                    .attributes(
                        Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.CURRENT.toString())
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
                        .attributes(
                            Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.CURRENT.toString())
                        )
                        .build()
                );
        }

        if (transformLocalOnlyNodes) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-0-tasks-transform-remote-disabled")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.TRANSFORM_ROLE))
                    .attributes(
                        Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.CURRENT.toString())
                    )
                    .build()
            );
        }

        if (currentDataNode) {
            nodes.add(
                DiscoveryNodeUtils.builder("current-data-node-with-transform-disabled")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                    .attributes(
                        Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.CURRENT.toString())
                    )
                    .build()
            );
        }

        return nodes;
    }

    private ClusterState buildClusterState(DiscoveryNodes.Builder nodes) {
        Metadata.Builder metadata = Metadata.builder().clusterUUID("cluster-uuid");
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);
        PersistentTasksCustomMetadata.Builder pTasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "transform-task-1",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", TransformConfigVersion.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-1-tasks", "")
            )
            .addTask(
                "transform-task-2",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-2", TransformConfigVersion.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-2-tasks", "")
            )
            .addTask(
                "transform-task-3",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-3", TransformConfigVersion.CURRENT, null, false),
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
        var transformServices = transformServices(
            new InMemoryTransformConfigManager(),
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO)
        );
        return buildTaskExecutor(transformServices);
    }

    private TransformServices transformServices(TransformConfigManager configManager, TransformScheduler scheduler) {
        var mockAuditor = mock(TransformAuditor.class);
        var transformCheckpointService = new TransformCheckpointService(
            Clock.systemUTC(),
            Settings.EMPTY,
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            ),
            configManager,
            mockAuditor
        );
        return new TransformServices(configManager, transformCheckpointService, mockAuditor, scheduler, mock(TransformNode.class));
    }

    private TransformPersistentTasksExecutor buildTaskExecutor(TransformServices transformServices) {
        return new TransformPersistentTasksExecutor(
            mock(Client.class),
            transformServices,
            threadPool,
            clusterService(),
            Settings.EMPTY,
            new DefaultTransformExtension(),
            TestIndexNameExpressionResolver.newInstance()
        );
    }

    private ClusterService clusterService() {
        var clusterService = mock(ClusterService.class);
        var cSettings = new ClusterSettings(Settings.EMPTY, Set.of(Transform.NUM_FAILURE_RETRIES_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
        when(clusterService.state()).thenReturn(TransformInternalIndexTests.randomTransformClusterState());
        return clusterService;
    }
}
