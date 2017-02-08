/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.CompletionPersistentTaskAction.Response;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.TestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentActionCoordinatorTests extends ESTestCase {

    private ClusterService createClusterService() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterService(Settings.builder().put("cluster.name", "PersistentActionExecutorTests").build(),
                clusterSettings, null, () -> new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(),
                Version.CURRENT));
    }

    private DiscoveryNodes createTestNodes(int nonLocalNodesCount, Settings settings) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNode.createLocal(settings, buildNewFakeTransportAddress(), "this_node"));
        for (int i = 0; i < nonLocalNodesCount; i++) {
            nodes.add(new DiscoveryNode("other_node_" + i, buildNewFakeTransportAddress(), Version.CURRENT));
        }
        nodes.localNodeId("this_node");
        return nodes.build();
    }

    public void testStartTask() throws Exception {
        ClusterService clusterService = createClusterService();
        PersistentActionService persistentActionService = mock(PersistentActionService.class);
        PersistentActionRegistry registry = new PersistentActionRegistry(Settings.EMPTY);
        @SuppressWarnings("unchecked") TransportPersistentAction<TestRequest> action = mock(TransportPersistentAction.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        registry.registerPersistentAction("test", action);

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        PersistentActionCoordinator coordinator = new PersistentActionCoordinator(Settings.EMPTY, persistentActionService,
                registry, new TaskManager(Settings.EMPTY), executor);

        ClusterState state = ClusterState.builder(clusterService.state()).nodes(createTestNodes(nonLocalNodesCount, Settings.EMPTY))
                .build();

        Map<Long, PersistentTaskInProgress<?>> tasks = new HashMap<>();
        long taskId = randomLong();
        boolean added = false;
        if (nonLocalNodesCount > 0) {
            for (int i = 0; i < randomInt(5); i++) {
                tasks.put(taskId, new PersistentTaskInProgress<>(taskId, "test_action", new TestRequest("other_" + i),
                        "other_node_" + randomInt(nonLocalNodesCount)));
                taskId++;
                if (added == false && randomBoolean()) {
                    added = true;
                    tasks.put(taskId, new PersistentTaskInProgress<>(taskId, "test", new TestRequest("this_param"), "this_node"));
                    taskId++;
                }
            }
        }

        if (added == false) {
            logger.info("No local node action was added");

        }
        ClusterState newClusterState = ClusterState.builder(state)
                .putCustom(PersistentTasksInProgress.TYPE, new PersistentTasksInProgress(taskId, tasks)).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));
        if (added) {
            // Action for this node was added, let's make sure it was invoked
            assertThat(executor.executions.size(), equalTo(1));

            // Add task on some other node
            state = newClusterState;
            newClusterState = addTask(state, "test", new TestRequest(), "some_other_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action wasn't called again
            assertThat(executor.executions.size(), equalTo(1));

            // Start another task on this node
            state = newClusterState;
            newClusterState = addTask(state, "test", new TestRequest("this_param"), "this_node");
            coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

            // Make sure action was called this time
            assertThat(executor.size(), equalTo(2));

            // Finish both tasks
            executor.get(0).listener.onFailure(new RuntimeException());
            executor.get(1).listener.onResponse(Empty.INSTANCE);
            long failedTaskId = executor.get(0).task.getParentTaskId().getId();
            long finishedTaskId = executor.get(1).task.getParentTaskId().getId();
            executor.clear();

            // Add task on some other node
            state = newClusterState;
            newClusterState = addTask(state, "test", new TestRequest(), "some_other_node");
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

    public void testTaskCancellation() {
        ClusterService clusterService = createClusterService();
        AtomicLong capturedTaskId = new AtomicLong();
        AtomicReference<ActionListener<CancelTasksResponse>> capturedListener = new AtomicReference<>();
        PersistentActionService persistentActionService = new PersistentActionService(Settings.EMPTY, null, null) {
            @Override
            public void sendCancellation(long taskId, ActionListener<CancelTasksResponse> listener) {
                capturedTaskId.set(taskId);
                capturedListener.set(listener);
            }
        };
        PersistentActionRegistry registry = new PersistentActionRegistry(Settings.EMPTY);
        @SuppressWarnings("unchecked") TransportPersistentAction<TestRequest> action = mock(TransportPersistentAction.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        registry.registerPersistentAction("test", action);

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        TaskManager taskManager = new TaskManager(Settings.EMPTY);
        PersistentActionCoordinator coordinator = new PersistentActionCoordinator(Settings.EMPTY, persistentActionService,
                registry, taskManager, executor);

        ClusterState state = ClusterState.builder(clusterService.state()).nodes(createTestNodes(nonLocalNodesCount, Settings.EMPTY))
                .build();

        ClusterState newClusterState = state;
        // Allocate first task
        state = newClusterState;
        newClusterState = addTask(state, "test", new TestRequest(), "this_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check the the task is know to the task manager
        assertThat(taskManager.getTasks().size(), equalTo(1));
        Task runningTask = taskManager.getTasks().values().iterator().next();
        long persistentId = runningTask.getParentTaskId().getId();
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
        assertThat(taskManager.getTasks().values().iterator().next().getStatus().toString(), equalTo("{\"state\":\"CANCELLED\"}"));


        // That should trigger cancellation request
        assertThat(capturedTaskId.get(), equalTo(localId));
        // Notify successful cancellation
        capturedListener.get().onResponse(new CancelTasksResponse());

        // finish or fail task
        if (randomBoolean()) {
            executor.get(0).listener.onResponse(Empty.INSTANCE);
        } else {
            executor.get(0).listener.onFailure(new IOException("test"));
        }

        // Check the the task is now removed from task manager
        assertThat(taskManager.getTasks().values(), empty());

    }

    public void testNotificationFailure() {
        ClusterService clusterService = createClusterService();
        AtomicLong capturedTaskId = new AtomicLong(-1L);
        AtomicReference<Exception> capturedException = new AtomicReference<>();
        AtomicReference<ActionListener<Response>> capturedListener = new AtomicReference<>();
        PersistentActionService persistentActionService = new PersistentActionService(Settings.EMPTY, clusterService, null) {
            @Override
            public void sendCompletionNotification(long taskId, Exception failure, ActionListener<Response> listener) {
                capturedTaskId.set(taskId);
                capturedException.set(failure);
                capturedListener.set(listener);
            }
        };
        PersistentActionRegistry registry = new PersistentActionRegistry(Settings.EMPTY);
        @SuppressWarnings("unchecked") TransportPersistentAction<TestRequest> action = mock(TransportPersistentAction.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        registry.registerPersistentAction("test", action);

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        TaskManager taskManager = new TaskManager(Settings.EMPTY);
        PersistentActionCoordinator coordinator = new PersistentActionCoordinator(Settings.EMPTY, persistentActionService,
                registry, taskManager, executor);

        ClusterState state = ClusterState.builder(clusterService.state()).nodes(createTestNodes(nonLocalNodesCount, Settings.EMPTY))
                .build();

        ClusterState newClusterState = state;
        // Allocate first task
        state = newClusterState;
        newClusterState = addTask(state, "test", new TestRequest(), "this_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Fail the task
        executor.get(0).listener.onFailure(new RuntimeException("test failure"));

        // Check that notification was sent
        assertThat(capturedException.get().getMessage(), equalTo("test failure"));
        capturedException.set(null);

        // Simulate failure to notify
        capturedListener.get().onFailure(new IOException("simulated notification failure"));

        // Allocate another task
        state = newClusterState;
        newClusterState = addTask(state, "test", new TestRequest(), "other_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check that notification was sent again
        assertThat(capturedException.get().getMessage(), equalTo("test failure"));

        // Check the the task is still known by the task manager
        assertThat(taskManager.getTasks().size(), equalTo(1));
        long id = taskManager.getTasks().values().iterator().next().getParentTaskId().getId();

        // This time acknowledge notification
        capturedListener.get().onResponse(new Response());

        // Reallocate failed task to another node
        state = newClusterState;
        newClusterState = reallocateTask(state, id, "other_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check the the task is now removed from task manager
        assertThat(taskManager.getTasks().values(), empty());

    }

    private <Request extends PersistentActionRequest> ClusterState addTask(ClusterState state, String action, Request request,
                                                                           String node) {
        PersistentTasksInProgress prevTasks = state.custom(PersistentTasksInProgress.TYPE);
        Map<Long, PersistentTaskInProgress<?>> tasks = prevTasks == null ? new HashMap<>() : new HashMap<>(prevTasks.taskMap());
        long id =  prevTasks == null ? 0 : prevTasks.getCurrentId();
        tasks.put(id, new PersistentTaskInProgress<>(id, action, request, node));
        return ClusterState.builder(state).putCustom(PersistentTasksInProgress.TYPE,
                new PersistentTasksInProgress(prevTasks == null ? 1 : prevTasks.getCurrentId() + 1, tasks)).build();
    }

    private ClusterState reallocateTask(ClusterState state, long taskId, String node) {
        PersistentTasksInProgress prevTasks = state.custom(PersistentTasksInProgress.TYPE);
        assertNotNull(prevTasks);
        Map<Long, PersistentTaskInProgress<?>> tasks = new HashMap<>(prevTasks.taskMap());
        PersistentTaskInProgress<?> prevTask = tasks.get(taskId);
        assertNotNull(prevTask);
        tasks.put(prevTask.getId(), new PersistentTaskInProgress<>(prevTask, node));
        return ClusterState.builder(state).putCustom(PersistentTasksInProgress.TYPE,
                new PersistentTasksInProgress(prevTasks.getCurrentId(), tasks)).build();
    }

    private ClusterState removeTask(ClusterState state, long taskId) {
        PersistentTasksInProgress prevTasks = state.custom(PersistentTasksInProgress.TYPE);
        assertNotNull(prevTasks);
        Map<Long, PersistentTaskInProgress<?>> tasks = new HashMap<>(prevTasks.taskMap());
        PersistentTaskInProgress<?> prevTask = tasks.get(taskId);
        assertNotNull(prevTask);
        tasks.remove(prevTask.getId());
        return ClusterState.builder(state).putCustom(PersistentTasksInProgress.TYPE,
                new PersistentTasksInProgress(prevTasks.getCurrentId(), tasks)).build();
    }

    private class Execution {
        private final PersistentActionRequest request;
        private final PersistentTask task;
        private final PersistentActionRegistry.PersistentActionHolder<?> holder;
        private final ActionListener<Empty> listener;

        Execution(PersistentActionRequest request, PersistentTask task, PersistentActionRegistry.PersistentActionHolder<?> holder,
                         ActionListener<Empty> listener) {
            this.request = request;
            this.task = task;
            this.holder = holder;
            this.listener = listener;
        }
    }

    private class MockExecutor extends PersistentActionExecutor {
        private List<Execution> executions = new ArrayList<>();

        MockExecutor() {
            super(null);
        }

        @Override
        public <Request extends PersistentActionRequest> void executeAction(Request request, PersistentTask task,
                                                                            PersistentActionRegistry.PersistentActionHolder<Request> holder,
                                                                            ActionListener<Empty> listener) {
            executions.add(new Execution(request, task, holder, listener));
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
