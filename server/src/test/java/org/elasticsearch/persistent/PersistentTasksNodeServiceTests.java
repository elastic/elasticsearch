/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentTasksNodeServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private PersistentTasksExecutor.Scope scope;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        scope = randomFrom(PersistentTasksExecutor.Scope.values());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private ClusterState createInitialClusterState(int nonLocalNodesCount, Settings settings) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("PersistentActionExecutorTests"));
        state.metadata(Metadata.builder().generateClusterUuidIfNeeded());
        state.routingTable(RoutingTable.builder().build());
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNodeUtils.builder("this_node").applySettings(settings).build());
        for (int i = 0; i < nonLocalNodesCount; i++) {
            nodes.add(DiscoveryNodeUtils.create("other_node_" + i));
        }
        nodes.localNodeId("this_node");
        state.nodes(nodes);
        return state.build();
    }

    public void testStartTask() {
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        @SuppressWarnings("unchecked")
        PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        when(action.scope()).thenReturn(scope);
        int nonLocalNodesCount = randomInt(10);
        // need to account for 5 original tasks on each node and their relocations
        for (int i = 0; i < (nonLocalNodesCount + 1) * 10; i++) {
            TaskId parentId = new TaskId("cluster", i);
            when(action.createTask(anyLong(), anyString(), anyString(), eq(parentId), any(), any())).thenReturn(
                new TestPersistentTasksPlugin.TestTask(i, "persistent", "test", "", parentId, Collections.emptyMap())
            );
        }
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(
            threadPool,
            persistentTasksService,
            registry,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            executor
        );

        ClusterState state = createInitialClusterState(nonLocalNodesCount, Settings.EMPTY);

        var tasks = tasksBuilder(null);
        boolean added = false;
        if (nonLocalNodesCount > 0) {
            for (int i = 0; i < randomInt(5); i++) {
                tasks.addTask(
                    UUIDs.base64UUID(),
                    TestPersistentTasksExecutor.NAME,
                    new TestParams("other_" + i),
                    new Assignment("other_node_" + randomInt(nonLocalNodesCount), "test assignment on other node")
                );
                if (added == false && randomBoolean()) {
                    added = true;
                    tasks.addTask(
                        UUIDs.base64UUID(),
                        TestPersistentTasksExecutor.NAME,
                        new TestParams("this_param"),
                        new Assignment("this_node", "test assignment on this node")
                    );
                }
            }
        }

        if (added == false) {
            logger.info("No local node action was added");
        }

        Metadata.Builder metadata = Metadata.builder(state.metadata());
        updateTasksCustomMetadata(metadata, tasks);
        ClusterState newClusterState = ClusterState.builder(state).metadata(metadata).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));
        if (added) {
            // Action for this node was added, let's make sure it was invoked
            assertThat(executor.executions.size(), equalTo(1));

            // Add task on some other node
            state = newClusterState;
            newClusterState = addTask(state, TestPersistentTasksExecutor.NAME, null, "some_other_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action wasn't called again
            assertThat(executor.executions.size(), equalTo(1));
            assertThat(executor.get(0).task.isCompleted(), is(false));

            // Start another task on this node
            state = newClusterState;
            newClusterState = addTask(state, TestPersistentTasksExecutor.NAME, new TestParams("this_param"), "this_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action was called this time
            assertThat(executor.size(), equalTo(2));
            assertThat(executor.get(1).task.isCompleted(), is(false));

            // Finish both tasks
            executor.get(0).task.markAsFailed(new RuntimeException());
            executor.get(1).task.markAsCompleted();

            assertThat(executor.get(0).task.isCompleted(), is(true));
            assertThat(executor.get(1).task.isCompleted(), is(true));

            String failedTaskId = executor.get(0).task.getPersistentTaskId();
            String finishedTaskId = executor.get(1).task.getPersistentTaskId();
            executor.clear();

            // Add task on some other node
            state = newClusterState;
            newClusterState = addTask(state, TestPersistentTasksExecutor.NAME, null, "some_other_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action wasn't called again
            assertThat(executor.size(), equalTo(0));

            // Simulate reallocation of the failed task on the same node
            state = newClusterState;
            newClusterState = reallocateTask(state, failedTaskId, "this_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Simulate removal of the finished task
            state = newClusterState;
            newClusterState = removeTask(state, finishedTaskId);
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action was only allocated on this node once
            assertThat(executor.size(), equalTo(1));
        }
    }

    public void testParamsStatusAndNodeTaskAreDelegated() throws Exception {
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        @SuppressWarnings("unchecked")
        PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        when(action.scope()).thenReturn(scope);
        TaskId parentId = new TaskId("cluster", 1);
        AllocatedPersistentTask nodeTask = new TestPersistentTasksPlugin.TestTask(
            0,
            "persistent",
            "test",
            "",
            parentId,
            Collections.emptyMap()
        );
        when(action.createTask(anyLong(), anyString(), anyString(), eq(parentId), any(), any())).thenReturn(nodeTask);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(
            threadPool,
            persistentTasksService,
            registry,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            executor
        );

        ClusterState state = createInitialClusterState(1, Settings.EMPTY);

        PersistentTaskState taskState = new TestPersistentTasksPlugin.State("_test_phase");
        var tasks = tasksBuilder(null);
        String taskId = UUIDs.base64UUID();
        TestParams taskParams = new TestParams("other_0");
        tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, taskParams, new Assignment("this_node", "test assignment on other node"));
        tasks.updateTaskState(taskId, taskState);
        Metadata.Builder metadata = Metadata.builder(state.metadata());
        updateTasksCustomMetadata(metadata, tasks);
        ClusterState newClusterState = ClusterState.builder(state).metadata(metadata).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        assertThat(executor.size(), equalTo(1));
        assertThat(executor.get(0).params, sameInstance(taskParams));
        assertThat(executor.get(0).state, sameInstance(taskState));
        assertThat(executor.get(0).task, sameInstance(nodeTask));
    }

    public void testTaskCancellation() {
        AtomicLong capturedTaskId = new AtomicLong();
        AtomicReference<ActionListener<ListTasksResponse>> capturedListener = new AtomicReference<>();
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        PersistentTasksService persistentTasksService = new PersistentTasksService(null, null, client) {
            @Override
            void sendCancelRequest(final long taskId, final String reason, final ActionListener<ListTasksResponse> listener) {
                capturedTaskId.set(taskId);
                capturedListener.set(listener);
            }

            @Override
            public void sendCompletionRequest(
                final String taskId,
                final long taskAllocationId,
                final Exception taskFailure,
                final String localAbortReason,
                final TimeValue timeout,
                final ActionListener<PersistentTask<?>> listener
            ) {
                fail("Shouldn't be called during Cluster State cancellation");
            }
        };
        @SuppressWarnings("unchecked")
        PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(action.getTaskName()).thenReturn("test");
        when(action.createTask(anyLong(), anyString(), anyString(), any(), any(), any())).thenReturn(
            new TestPersistentTasksPlugin.TestTask(1, "persistent", "test", "", new TaskId("cluster", 1), Collections.emptyMap())
        );
        when(action.scope()).thenReturn(scope);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(
            threadPool,
            persistentTasksService,
            registry,
            taskManager,
            executor
        );

        ClusterState state = createInitialClusterState(nonLocalNodesCount, Settings.EMPTY);

        ClusterState newClusterState = state;
        // Allocate first task
        state = newClusterState;
        newClusterState = addTask(state, "test", null, "this_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check the task is know to the task manager
        assertThat(taskManager.getTasks().size(), equalTo(1));
        AllocatedPersistentTask runningTask = (AllocatedPersistentTask) taskManager.getTasks().values().iterator().next();
        String persistentId = runningTask.getPersistentTaskId();
        long localId = runningTask.getId();
        // Make sure it returns correct status
        Task.Status status = runningTask.getStatus();
        assertThat(status.toString(), equalTo("{\"state\":\"STARTED\"}"));

        state = newClusterState;
        // Relocate the task to some other node or remove it completely
        if (randomBoolean()) {
            newClusterState = reallocateTask(state, persistentId, "some_other_node");
        } else {
            newClusterState = removeTask(state, persistentId);
        }
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Make sure it returns correct status
        assertThat(taskManager.getTasks().size(), equalTo(1));
        assertThat(taskManager.getTasks().values().iterator().next().getStatus().toString(), equalTo("{\"state\":\"PENDING_CANCEL\"}"));

        // That should trigger cancellation request
        assertThat(capturedTaskId.get(), equalTo(localId));
        // Notify successful cancellation
        capturedListener.get().onResponse(new ListTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));

        // finish or fail task
        if (randomBoolean()) {
            executor.get(0).task.markAsCompleted();
        } else {
            executor.get(0).task.markAsFailed(new IOException("test"));
        }

        // Check the task is now removed from task manager
        assertThat(taskManager.getTasks().values(), empty());
    }

    public void testTaskLocalAbort() {
        AtomicReference<String> capturedTaskId = new AtomicReference<>();
        AtomicReference<ActionListener<PersistentTask<?>>> capturedListener = new AtomicReference<>();
        AtomicReference<String> capturedLocalAbortReason = new AtomicReference<>();
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        PersistentTasksService persistentTasksService = new PersistentTasksService(null, null, client) {
            @Override
            void sendCancelRequest(final long taskId, final String reason, final ActionListener<ListTasksResponse> listener) {
                fail("Shouldn't be called during local abort");
            }

            @Override
            public void sendCompletionRequest(
                final String taskId,
                final long taskAllocationId,
                final Exception taskFailure,
                final String localAbortReason,
                final TimeValue timeout,
                final ActionListener<PersistentTask<?>> listener
            ) {
                assertThat(taskId, not(nullValue()));
                assertThat(capturedTaskId.get(), nullValue());
                capturedTaskId.set(taskId);
                capturedListener.set(listener);
                capturedLocalAbortReason.set(localAbortReason);
                assertThat(taskFailure, nullValue());
            }
        };
        @SuppressWarnings("unchecked")
        PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(action.getTaskName()).thenReturn("test");
        when(action.createTask(anyLong(), anyString(), anyString(), any(), any(), any())).thenReturn(
            new TestPersistentTasksPlugin.TestTask(1, "persistent", "test", "", new TaskId("cluster", 1), Collections.emptyMap())
        );
        when(action.scope()).thenReturn(scope);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(
            threadPool,
            persistentTasksService,
            registry,
            taskManager,
            executor
        );

        ClusterState state = createInitialClusterState(nonLocalNodesCount, Settings.EMPTY);

        // Allocate first task
        ClusterState newClusterState = addTask(state, "test", null, "this_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check the task is known to the task manager
        assertThat(taskManager.getTasks().size(), equalTo(1));
        AllocatedPersistentTask runningTask = (AllocatedPersistentTask) taskManager.getTasks().values().iterator().next();
        String persistentId = runningTask.getPersistentTaskId();
        // Make sure it returns correct status
        Task.Status status = runningTask.getStatus();
        assertThat(status.toString(), equalTo("{\"state\":\"STARTED\"}"));

        // Abort the task locally
        runningTask.markAsLocallyAborted("testing local abort");

        // That should trigger an unassignment request
        assertThat(capturedTaskId.get(), equalTo(persistentId));
        assertThat(capturedLocalAbortReason.get(), equalTo("testing local abort"));
        // Notify successful unassignment
        var tasks = getPersistentTasks(newClusterState);
        capturedListener.get().onResponse(tasks.getTask(persistentId));

        // Check the task is now removed from the local task manager
        assertThat(taskManager.getTasks().values(), empty());

        // Check that races where some other event occurs after local abort are handled as expected
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                IllegalStateException e0 = expectThrows(IllegalStateException.class, runningTask::markAsCompleted);
                assertThat(
                    e0.getMessage(),
                    equalTo("attempt to complete task [test] with id [" + persistentId + "] which has been locally aborted")
                );
            }
            case 1 -> {
                IllegalStateException e1 = expectThrows(
                    IllegalStateException.class,
                    () -> runningTask.markAsFailed(new Exception("failure detected after local abort"))
                );
                assertThat(
                    e1.getMessage(),
                    equalTo("attempt to fail task [test] with id [" + persistentId + "] which has been locally aborted")
                );
            }
            case 2 -> {
                IllegalStateException e2 = expectThrows(
                    IllegalStateException.class,
                    () -> runningTask.markAsLocallyAborted("second local abort")
                );
                assertThat(
                    e2.getMessage(),
                    equalTo("attempt to locally abort task [test] with id [" + persistentId + "] which has already been locally aborted")
                );
            }
        }

        assertFalse(runningTask.markAsCancelled());
    }

    public void testRegisterTaskFails() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        final Client mockClient = mock(Client.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockThreadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);

        PersistentTasksService persistentTasksService = new PersistentTasksService(null, null, mockClient) {
            @Override
            public void sendCompletionRequest(
                String taskId,
                long taskAllocationId,
                Exception taskFailure,
                String localAbortReason,
                TimeValue timeout,
                ActionListener<PersistentTask<?>> listener
            ) {
                assertThat(taskFailure, instanceOf(RuntimeException.class));
                assertThat(taskFailure.getMessage(), equalTo("Something went wrong"));
                assertThat(localAbortReason, nullValue());
                listener.onResponse(mock(PersistentTask.class));
                latch.countDown();
            }
        };

        @SuppressWarnings("unchecked")
        PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        when(action.createTask(anyLong(), anyString(), anyString(), any(), any(), any())).thenThrow(
            new RuntimeException("Something went wrong")
        );
        when(action.scope()).thenReturn(scope);

        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(
            mockThreadPool,
            persistentTasksService,
            registry,
            new TaskManager(Settings.EMPTY, mockThreadPool, Collections.emptySet()),
            executor
        );

        ClusterState state = createInitialClusterState(0, Settings.EMPTY);

        var tasks = tasksBuilder(null);

        tasks.addTask(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            new TestParams("this_param"),
            new Assignment("this_node", "test assignment on this node")
        );

        Metadata.Builder metadata = Metadata.builder(state.metadata());
        updateTasksCustomMetadata(metadata, tasks);
        ClusterState newClusterState = ClusterState.builder(state).metadata(metadata).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Failed to start the task, make sure it wasn't invoked further
        assertThat(executor.executions.size(), equalTo(0));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private <Params extends PersistentTaskParams> ClusterState addTask(ClusterState state, String action, Params params, String node) {
        var builder = tasksBuilder(getPersistentTasks(state));
        return ClusterState.builder(state)
            .metadata(
                updateTasksCustomMetadata(
                    Metadata.builder(state.metadata()),
                    builder.addTask(UUIDs.base64UUID(), action, params, new Assignment(node, "test assignment"))
                )
            )
            .build();
    }

    private ClusterState reallocateTask(ClusterState state, String taskId, String node) {
        var builder = tasksBuilder(getPersistentTasks(state));
        assertTrue(builder.hasTask(taskId));
        return ClusterState.builder(state)
            .metadata(
                updateTasksCustomMetadata(
                    Metadata.builder(state.metadata()),
                    builder.reassignTask(taskId, new Assignment(node, "test assignment"))
                )
            )
            .build();
    }

    private ClusterState removeTask(ClusterState state, String taskId) {
        var builder = tasksBuilder(getPersistentTasks(state));
        assertTrue(builder.hasTask(taskId));
        return ClusterState.builder(state)
            .metadata(updateTasksCustomMetadata(Metadata.builder(state.metadata()), builder.removeTask(taskId)))
            .build();
    }

    private PersistentTasks getPersistentTasks(ClusterState clusterState) {
        if (scope == PersistentTasksExecutor.Scope.CLUSTER) {
            return ClusterPersistentTasksCustomMetadata.get(clusterState.metadata());
        } else {
            return PersistentTasksCustomMetadata.get(clusterState.metadata().getProject());
        }
    }

    private Metadata.Builder updateTasksCustomMetadata(Metadata.Builder metadata, PersistentTasks.Builder<?> tasksBuilder) {
        if (scope == PersistentTasksExecutor.Scope.CLUSTER) {
            metadata.putCustom(ClusterPersistentTasksCustomMetadata.TYPE, (ClusterPersistentTasksCustomMetadata) tasksBuilder.build());
        } else {
            metadata.putCustom(PersistentTasksCustomMetadata.TYPE, (PersistentTasksCustomMetadata) tasksBuilder.build());
        }
        return metadata;
    }

    private PersistentTasks.Builder<?> tasksBuilder(PersistentTasks tasks) {
        if (tasks == null) {
            return scope == PersistentTasksExecutor.Scope.CLUSTER
                ? ClusterPersistentTasksCustomMetadata.builder()
                : PersistentTasksCustomMetadata.builder();
        } else {
            return tasks.toBuilder();
        }
    }

    private static class Execution {

        private final PersistentTaskParams params;
        private final AllocatedPersistentTask task;
        private final PersistentTaskState state;

        Execution(PersistentTaskParams params, AllocatedPersistentTask task, PersistentTaskState state) {
            this.params = params;
            this.task = task;
            this.state = state;
        }
    }

    private class MockExecutor extends NodePersistentTasksExecutor {
        private final List<Execution> executions = new ArrayList<>();

        @Override
        public <Params extends PersistentTaskParams> void executeTask(
            final Params params,
            final PersistentTaskState state,
            final AllocatedPersistentTask task,
            final PersistentTasksExecutor<Params> executor
        ) {
            executions.add(new Execution(params, task, state));
        }

        public Execution get(int i) {
            return executions.get(i);
        }

        public int size() {
            return executions.size();
        }

        public void clear() {
            executions.clear();
        }
    }

}
