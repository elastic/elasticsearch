/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentTasksNodeServiceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
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
        nodes.add(DiscoveryNode.createLocal(settings, buildNewFakeTransportAddress(), "this_node"));
        for (int i = 0; i < nonLocalNodesCount; i++) {
            nodes.add(new DiscoveryNode("other_node_" + i, buildNewFakeTransportAddress(), Version.CURRENT));
        }
        nodes.localNodeId("this_node");
        state.nodes(nodes);
        return state.build();
    }

    public void testStartTask() {
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        @SuppressWarnings("unchecked") PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        int nonLocalNodesCount = randomInt(10);
        // need to account for 5 original tasks on each node and their relocations
        for (int i = 0; i < (nonLocalNodesCount + 1) * 10; i++) {
            TaskId parentId = new TaskId("cluster", i);
            when(action.createTask(anyLong(), anyString(), anyString(), eq(parentId), any(), any())).thenReturn(
                    new TestPersistentTasksPlugin.TestTask(i, "persistent", "test", "", parentId, Collections.emptyMap()));
        }
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(persistentTasksService,
                registry, new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()), executor);

        ClusterState state = createInitialClusterState(nonLocalNodesCount, Settings.EMPTY);

        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        boolean added = false;
        if (nonLocalNodesCount > 0) {
            for (int i = 0; i < randomInt(5); i++) {
                tasks.addTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("other_" + i),
                        new Assignment("other_node_" + randomInt(nonLocalNodesCount), "test assignment on other node"));
                if (added == false && randomBoolean()) {
                    added = true;
                    tasks.addTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("this_param"),
                            new Assignment("this_node", "test assignment on this node"));
                }
            }
        }

        if (added == false) {
            logger.info("No local node action was added");
        }

        Metadata.Builder metadata = Metadata.builder(state.metadata());
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
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
        @SuppressWarnings("unchecked") PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        TaskId parentId = new TaskId("cluster", 1);
        AllocatedPersistentTask nodeTask =
                new TestPersistentTasksPlugin.TestTask(0, "persistent", "test", "", parentId, Collections.emptyMap());
        when(action.createTask(anyLong(), anyString(), anyString(), eq(parentId), any(), any())).thenReturn(nodeTask);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(persistentTasksService,
                registry, new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()), executor);

        ClusterState state = createInitialClusterState(1, Settings.EMPTY);

        PersistentTaskState taskState = new TestPersistentTasksPlugin.State("_test_phase");
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        String taskId = UUIDs.base64UUID();
        TestParams taskParams = new TestParams("other_0");
        tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, taskParams, new Assignment("this_node", "test assignment on other node"));
        tasks.updateTaskState(taskId, taskState);
        Metadata.Builder metadata = Metadata.builder(state.metadata());
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        ClusterState newClusterState = ClusterState.builder(state).metadata(metadata).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        assertThat(executor.size(), equalTo(1));
        assertThat(executor.get(0).params, sameInstance(taskParams));
        assertThat(executor.get(0).state, sameInstance(taskState));
        assertThat(executor.get(0).task, sameInstance(nodeTask));
    }

    public void testTaskCancellation() {
        AtomicLong capturedTaskId = new AtomicLong();
        AtomicReference<ActionListener<CancelTasksResponse>> capturedListener = new AtomicReference<>();
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        PersistentTasksService persistentTasksService = new PersistentTasksService(null, null, client) {
            @Override
            void sendCancelRequest(final long taskId, final String reason, final ActionListener<CancelTasksResponse> listener) {
                capturedTaskId.set(taskId);
                capturedListener.set(listener);
            }

            @Override
            public void sendCompletionRequest(final String taskId, final long taskAllocationId,
                                              final Exception taskFailure, final ActionListener<PersistentTask<?>> listener) {
                fail("Shouldn't be called during Cluster State cancellation");
            }
        };
        @SuppressWarnings("unchecked") PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        when(action.getTaskName()).thenReturn("test");
        when(action.createTask(anyLong(), anyString(), anyString(), any(), any(), any()))
                .thenReturn(new TestPersistentTasksPlugin.TestTask(1, "persistent", "test", "", new TaskId("cluster", 1),
                        Collections.emptyMap()));
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        int nonLocalNodesCount = randomInt(10);
        MockExecutor executor = new MockExecutor();
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(persistentTasksService,
                registry, taskManager, executor);

        ClusterState state = createInitialClusterState(nonLocalNodesCount, Settings.EMPTY);

        ClusterState newClusterState = state;
        // Allocate first task
        state = newClusterState;
        newClusterState = addTask(state, "test", null, "this_node");
        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Check the task is know to the task manager
        assertThat(taskManager.getTasks().size(), equalTo(1));
        AllocatedPersistentTask runningTask = (AllocatedPersistentTask)taskManager.getTasks().values().iterator().next();
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
        capturedListener.get().onResponse(
            new CancelTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));

        // finish or fail task
        if (randomBoolean()) {
            executor.get(0).task.markAsCompleted();
        } else {
            executor.get(0).task.markAsFailed(new IOException("test"));
        }

        // Check the task is now removed from task manager
        assertThat(taskManager.getTasks().values(), empty());
    }

    public void testRegisterTaskFails() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        final Client mockClient = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.newDirectExecutorService());
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);

        PersistentTasksService persistentTasksService = new PersistentTasksService(null, null, mockClient) {
            @Override
            public void sendCompletionRequest(String taskId, long taskAllocationId, Exception taskFailure,
                                              ActionListener<PersistentTask<?>> listener) {
                assertThat(taskFailure, instanceOf(RuntimeException.class));
                assertThat(taskFailure.getMessage(), equalTo("Something went wrong"));
                listener.onResponse(mock(PersistentTask.class));
                latch.countDown();
            }
        };

        @SuppressWarnings("unchecked") PersistentTasksExecutor<TestParams> action = mock(PersistentTasksExecutor.class);
        when(action.getExecutor()).thenReturn(ThreadPool.Names.SAME);
        when(action.getTaskName()).thenReturn(TestPersistentTasksExecutor.NAME);
        when(action.createTask(anyLong(), anyString(), anyString(), any(), any(), any()))
            .thenThrow(new RuntimeException("Something went wrong"));

        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(Collections.singletonList(action));

        MockExecutor executor = new MockExecutor();
        PersistentTasksNodeService coordinator = new PersistentTasksNodeService(persistentTasksService,
            registry, new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()), executor);

        ClusterState state = createInitialClusterState(0, Settings.EMPTY);

        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();

        tasks.addTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("this_param"),
            new Assignment("this_node", "test assignment on this node"));

        Metadata.Builder metadata = Metadata.builder(state.metadata());
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        ClusterState newClusterState = ClusterState.builder(state).metadata(metadata).build();

        coordinator.clusterChanged(new ClusterChangedEvent("test", newClusterState, state));

        // Failed to start the task, make sure it wasn't invoked further
        assertThat(executor.executions.size(), equalTo(0));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private <Params extends PersistentTaskParams> ClusterState addTask(ClusterState state, String action, Params params,
                                                                       String node) {
        PersistentTasksCustomMetadata.Builder builder =
                PersistentTasksCustomMetadata.builder(state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE));
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE,
                builder.addTask(UUIDs.base64UUID(), action, params, new Assignment(node, "test assignment")).build())).build();
    }

    private ClusterState reallocateTask(ClusterState state, String taskId, String node) {
        PersistentTasksCustomMetadata.Builder builder =
                PersistentTasksCustomMetadata.builder(state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE));
        assertTrue(builder.hasTask(taskId));
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE,
                builder.reassignTask(taskId, new Assignment(node, "test assignment")).build())).build();
    }

    private ClusterState removeTask(ClusterState state, String taskId) {
        PersistentTasksCustomMetadata.Builder builder =
                PersistentTasksCustomMetadata.builder(state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE));
        assertTrue(builder.hasTask(taskId));
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE,
                builder.removeTask(taskId).build())).build();
    }

    private class Execution {

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
        private List<Execution> executions = new ArrayList<>();

        MockExecutor() {
            super(null);
        }

        @Override
        public <Params extends PersistentTaskParams> void executeTask(final Params params,
                                                                      final PersistentTaskState state,
                                                                      final AllocatedPersistentTask task,
                                                                      final PersistentTasksExecutor<Params> executor) {
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
