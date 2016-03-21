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
package org.elasticsearch.action.admin.cluster.node.tasks;

import com.google.common.base.Function;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.tasks.MockTaskManagerListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for task management API
 * <p>
 * We need at least 2 nodes so we have a master node a non-master node
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class TasksIT extends ESIntegTestCase {

    private Map<Tuple<String, String>, RecordingTaskManagerListener> listeners = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class, TestTaskPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(MockTaskManager.USE_MOCK_TASK_MANAGER, true)
            .build();
    }

    public void testTaskCounts() {
        // Run only on data nodes
        ListTasksResponse response = client().admin().cluster().prepareListTasks("data:true").setActions(ListTasksAction.NAME + "[n]").get();
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(cluster().numDataNodes()));
    }

    public void testMasterNodeOperationTasks() {
        registerTaskManageListeners(ClusterHealthAction.NAME);

        // First run the health on the master node - should produce only one task on the master node
        internalCluster().masterClient().admin().cluster().prepareHealth().get();
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        })); // counting only registration events
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> event) {
                return event.v1() == false;
            }
        })); // counting only unregistration events

        resetTaskManageListeners(ClusterHealthAction.NAME);

        // Now run the health on a non-master node - should produce one task on master and one task on another node
        internalCluster().nonMasterClient().admin().cluster().prepareHealth().get();
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        })); // counting only registration events
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> event) {
                return event.v1() == false;
            }
        })); // counting only unregistration events
        List<TaskInfo> tasks = findEvents(ClusterHealthAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        });

        // Verify that one of these tasks is a parent of another task
        if (tasks.get(0).getParentTaskId().isSet()) {
            assertParentTask(Collections.singletonList(tasks.get(0)), tasks.get(1));
        } else {
            assertParentTask(Collections.singletonList(tasks.get(1)), tasks.get(0));
        }
    }

    public void testTransportReplicationAllShardsTasks() {
        registerTaskManageListeners(PercolateAction.NAME);  // main task
        registerTaskManageListeners(PercolateAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().preparePercolate().setIndices("test").setDocumentType("foo").setSource("{}").get();

        // the percolate operation should produce one main task
        NumShards numberOfShards = getNumShards("test");
        assertEquals(1, numberOfEvents(PercolateAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        // and then one operation per shard
        assertEquals(numberOfShards.numPrimaries, numberOfEvents(PercolateAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>,
            Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));

        // the shard level tasks should have the main task as a parent
        assertParentTask(findEvents(PercolateAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }), findEvents(PercolateAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }).get(0));
    }

    public void testTransportBroadcastByNodeTasks() {
        registerTaskManageListeners(UpgradeAction.NAME);  // main task
        registerTaskManageListeners(UpgradeAction.NAME + "[n]"); // node level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareUpgrade("test").get();

        // the percolate operation should produce one main task
        assertEquals(1, numberOfEvents(UpgradeAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        // and then one operation per each node where shards are located
        assertEquals(internalCluster().nodesInclude("test").size(), numberOfEvents(UpgradeAction.NAME + "[n]", new
            Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));

        // all node level tasks should have the main task as a parent
        assertParentTask(findEvents(UpgradeAction.NAME + "[n]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }), findEvents(UpgradeAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }).get(0));
    }

    public void testTransportReplicationSingleShardTasks() {
        registerTaskManageListeners(ValidateQueryAction.NAME);  // main task
        registerTaskManageListeners(ValidateQueryAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareValidateQuery("test").get();

        // the validate operation should produce one main task
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        // and then one operation
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        // the shard level operation should have the main task as its parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }), findEvents(ValidateQueryAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }).get(0));
    }

    public void testTransportBroadcastReplicationTasks() {
        registerTaskManageListeners(RefreshAction.NAME);  // main task
        registerTaskManageListeners(RefreshAction.NAME + "[s]"); // shard level tasks
        registerTaskManageListeners(RefreshAction.NAME + "[s][*]"); // primary and replica shard tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareRefresh("test").get();

        // the refresh operation should produce one main task
        NumShards numberOfShards = getNumShards("test");

        logger.debug("number of shards, total: [{}], primaries: [{}] ", numberOfShards.totalNumShards, numberOfShards.numPrimaries);
        logger.debug("main events {}", numberOfEvents(RefreshAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        logger.debug("main event node {}", findEvents(RefreshAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }).get(0).getNode().name());
        logger.debug("[s] events {}", numberOfEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        logger.debug("[s][*] events {}", numberOfEvents(RefreshAction.NAME + "[s][*]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        logger.debug("nodes with the index {}", internalCluster().nodesInclude("test"));

        assertEquals(1, numberOfEvents(RefreshAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));
        // Because it's broadcast replication action we will have as many [s] level requests
        // as we have primary shards on the coordinating node plus we will have one task per primary outside of the
        // coordinating node due to replication.
        // If all primaries are on the coordinating node, the number of tasks should be equal to the number of primaries
        // If all primaries are not on the coordinating node, the number of tasks should be equal to the number of primaries times 2
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }), greaterThanOrEqualTo(numberOfShards.numPrimaries));
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }), lessThanOrEqualTo(numberOfShards.numPrimaries * 2));

        // Verify that all [s] events have the proper parent
        // This is complicated because if the shard task runs on the same node it has main task as a parent
        // but if it runs on non-coordinating node it would have another intermediate [s] task on the coordinating node as a parent
        final TaskInfo mainTask = findEvents(RefreshAction.NAME, new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }).get(0);
        List<TaskInfo> sTasks = findEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        });
        for (TaskInfo taskInfo : sTasks) {
            if (mainTask.getNode().equals(taskInfo.getNode())) {
                // This shard level task runs on the same node as a parent task - it should have the main task as a direct parent
                assertParentTask(Collections.singletonList(taskInfo), mainTask);
            } else {
                final String description = taskInfo.getDescription();
                // This shard level task runs on another node - it should have a corresponding shard level task on the node where main task is running
                List<TaskInfo> sTasksOnRequestingNode = findEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
                        @Override
                        public Boolean apply(Tuple<Boolean, TaskInfo> event) {
                            return event.v1() && mainTask.getNode().equals(event.v2().getNode()) && description.equals(event.v2()
                                .getDescription());
                        }
                    });
                // There should be only one parent task
                assertEquals(1, sTasksOnRequestingNode.size());
                assertParentTask(Collections.singletonList(taskInfo), sTasksOnRequestingNode.get(0));
            }
        }

        // we will have as many [s][p] and [s][r] tasks as we have primary and replica shards
        assertEquals(numberOfShards.totalNumShards, numberOfEvents(RefreshAction.NAME + "[s][*]", new Function<Tuple<Boolean, TaskInfo>,
            Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        }));

        // we the [s][p] and [s][r] tasks should have a corresponding [s] task on the same node as a parent
        List<TaskInfo> spEvents = findEvents(RefreshAction.NAME + "[s][*]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
            @Override
            public Boolean apply(Tuple<Boolean, TaskInfo> input) {
                return input.v1();
            }
        });
        for (final TaskInfo taskInfo : spEvents) {
            List<TaskInfo> sTask;
            if (taskInfo.getAction().endsWith("[s][p]")) {
                // A [s][p] level task should have a corresponding [s] level task on the same node
                sTask = findEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
                        @Override
                        public Boolean apply(Tuple<Boolean, TaskInfo> event) {
                            return event.v1() && taskInfo.getNode().equals(event.v2().getNode()) && taskInfo.getDescription().equals(event.v2().getDescription());
                        }
                    });
            } else {
                // A [s][r] level task should have a corresponding [s] level task on the a different node (where primary is located)
                sTask = findEvents(RefreshAction.NAME + "[s]", new Function<Tuple<Boolean, TaskInfo>, Boolean>() {
                        @Override
                        public Boolean apply(Tuple<Boolean, TaskInfo> event) {
                            return event.v1() && taskInfo.getParentTaskId().getNodeId().equals(event.v2().getNode().getId()) && taskInfo
                                .getDescription()
                                .equals(event.v2().getDescription());
                        }
                    });
            }
            // There should be only one parent task
            assertEquals(1, sTask.size());
            assertParentTask(Collections.singletonList(taskInfo), sTask.get(0));
        }
    }

    /**
     * Very basic "is it plugged in" style test that indexes a document and
     * makes sure that you can fetch the status of the process. The goal here is
     * to verify that the large moving parts that make fetching task status work
     * fit together rather than to verify any particular status results from
     * indexing. For that, look at
     * {@link org.elasticsearch.action.support.replication.TransportReplicationActionTests}
     * . We intentionally don't use the task recording mechanism used in other
     * places in this test so we can make sure that the status fetching works
     * properly over the wire.
     */
    public void testCanFetchIndexStatus() throws InterruptedException, ExecutionException, IOException {
        /*
         * We prevent any tasks from unregistering until the test is done so we
         * can fetch them. This will gum up the server if we leave it enabled
         * but we'll be quick so it'll be OK (TM).
         */
        final ReentrantLock taskFinishLock = new ReentrantLock();
        taskFinishLock.lock();
        final CountDownLatch taskRegistered = new CountDownLatch(1);
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ((MockTaskManager)clusterService.getTaskManager()).addListener(new MockTaskManagerListener() {
                @Override
                public void onTaskRegistered(Task task) {
                    if (task.getAction().startsWith(IndexAction.NAME)) {
                        taskRegistered.countDown();
                    }
                }

                @Override
                public void onTaskUnregistered(Task task) {
                    /*
                     * We can't block all tasks here or the task listing task
                     * would never return.
                     */
                    if (false == task.getAction().startsWith(IndexAction.NAME)) {
                        return;
                    }
                    logger.debug("Blocking {} from being unregistered", task);
                    taskFinishLock.lock();
                    taskFinishLock.unlock();
                }
            });
        }
        ListenableActionFuture<?> indexFuture = client().prepareIndex("test", "test").setSource("test", "test").execute();
        taskRegistered.await(10, TimeUnit.SECONDS); // waiting for at least one task to be registered
        ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions("indices:data/write/index*").setDetailed(true)
                .get();
        taskFinishLock.unlock();
        indexFuture.get();
        assertThat(tasks.getTasks(), not(emptyCollectionOf(TaskInfo.class)));
        for (TaskInfo task : tasks.getTasks()) {
            assertNotNull(task.getStatus());
        }
    }

    public void testTasksCancellation() throws Exception {
        // Start blocking test task
        // Get real client (the plugin is not registered on transport nodes)
        ListenableActionFuture<TestTaskPlugin.NodesResponse> future = TestTaskPlugin.TestTaskAction.INSTANCE.newRequestBuilder(client()).execute();
        logger.info("--> started test tasks");

        // Wait for the task to start on all nodes
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertEquals(internalCluster().numDataAndMasterNodes(),
                    client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]")
                        .get().getTasks().size());
            }
        });

        logger.info("--> cancelling the main test task");
        CancelTasksResponse cancelTasksResponse = client().admin().cluster().prepareCancelTasks().setActions(TestTaskPlugin.TestTaskAction.NAME).get();
        assertEquals(1, cancelTasksResponse.getTasks().size());

        future.get();

        logger.info("--> checking that test tasks are not running");
        assertEquals(0, client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "*").get().getTasks().size());

    }

    public void testTasksUnblocking() throws Exception {
        // Start blocking test task
        ListenableActionFuture<TestTaskPlugin.NodesResponse> future = TestTaskPlugin.TestTaskAction.INSTANCE.newRequestBuilder(client()).execute();
        // Wait for the task to start on all nodes
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertEquals(internalCluster().numDataAndMasterNodes(),
                    client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]")
                        .get().getTasks().size());
            }
        });

        TestTaskPlugin.UnblockTestTasksAction.INSTANCE.newRequestBuilder(client()).get();

        future.get();
        assertEquals(0, client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size());
    }

    public void testTasksListWaitForCompletion() throws Exception {
        // Start blocking test task
        ListenableActionFuture<TestTaskPlugin.NodesResponse> future = TestTaskPlugin.TestTaskAction.INSTANCE.newRequestBuilder(client())
                .execute();

        ListenableActionFuture<ListTasksResponse> waitResponseFuture;
        try {
            // Wait for the task to start on all nodes
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertEquals(internalCluster().numDataAndMasterNodes(), client().admin().cluster().prepareListTasks()
                            .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size());
                }
            });

            // Spin up a request to wait for that task to finish
            waitResponseFuture = client().admin().cluster().prepareListTasks()
                    .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").setWaitForCompletion(true).execute();
        } finally {
            // Unblock the request so the wait for completion request can finish
            TestTaskPlugin.UnblockTestTasksAction.INSTANCE.newRequestBuilder(client()).get();
        }

        // Now that the task is unblocked the list response will come back
        ListTasksResponse waitResponse = waitResponseFuture.get();
        // If any tasks come back then they are the tasks we asked for - it'd be super weird if this wasn't true
        for (TaskInfo task: waitResponse.getTasks()) {
            assertEquals(task.getAction(), TestTaskPlugin.TestTaskAction.NAME + "[n]");
        }
        // See the next test to cover the timeout case

        future.get();
    }

    public void testTasksListWaitForTimeout() throws Exception {
        // Start blocking test task
        ListenableActionFuture<TestTaskPlugin.NodesResponse> future = TestTaskPlugin.TestTaskAction.INSTANCE.newRequestBuilder(client())
                .execute();
        try {
            // Wait for the task to start on all nodes
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertEquals(internalCluster().numDataAndMasterNodes(), client().admin().cluster().prepareListTasks()
                            .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size());
                }
            });

            // Spin up a request that should wait for those tasks to finish
            // It will timeout because we haven't unblocked the tasks
            ListTasksResponse waitResponse = client().admin().cluster().prepareListTasks()
                    .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").setWaitForCompletion(true).setTimeout(timeValueMillis(100))
                    .get();

            assertFalse(waitResponse.getNodeFailures().isEmpty());
            for (FailedNodeException failure : waitResponse.getNodeFailures()) {
                Throwable timeoutException = failure.getCause();
                // The exception sometimes comes back wrapped depending on the client
                if (timeoutException.getCause() != null) {
                    timeoutException = timeoutException.getCause();
                }
                if (false == (timeoutException instanceof ReceiveTimeoutTransportException
                        || timeoutException instanceof ElasticsearchTimeoutException)) {
                    fail("timeoutException should be a ReceiveTimeoutTransportException or ElasticsearchTimeoutException but was ["
                            + timeoutException + "]");
                }
            }
        } finally {
            // Now we can unblock those requests
            TestTaskPlugin.UnblockTestTasksAction.INSTANCE.newRequestBuilder(client()).get();
        }
        future.get();
    }

    public void testTasksListWaitForNoTask() throws Exception {
        // Spin up a request to wait for no matching tasks
        ListenableActionFuture<ListTasksResponse> waitResponseFuture = client().admin().cluster().prepareListTasks()
                .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").setWaitForCompletion(true).setTimeout(timeValueMillis(10))
                .execute();

        // It should finish quickly and without complaint
        assertThat(waitResponseFuture.get().getTasks(), emptyCollectionOf(TaskInfo.class));
    }

    public void testTasksWaitForAllTask() throws Exception {
        // Spin up a request to wait for all tasks in the cluster to make sure it doesn't cause an infinite loop
        ListTasksResponse response = client().admin().cluster().prepareListTasks().setWaitForCompletion(true)
            .setTimeout(timeValueSeconds(10)).get();

        // It should finish quickly and without complaint and list the list tasks themselves
        assertThat(response.getNodeFailures(), emptyCollectionOf(FailedNodeException.class));
        assertThat(response.getTaskFailures(), emptyCollectionOf(TaskOperationFailure.class));
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(1));
    }

    @Override
    public void tearDown() throws Exception {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            ((MockTaskManager)internalCluster().getInstance(ClusterService.class, entry.getKey().v1()).getTaskManager())
                    .removeListener(entry.getValue());
        }
        listeners.clear();
        super.tearDown();
    }

    /**
     * Registers recording task event listeners with the given action mask on all nodes
     */
    private void registerTaskManageListeners(String actionMasks) {
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            DiscoveryNode node = clusterService.localNode();
            RecordingTaskManagerListener listener = new RecordingTaskManagerListener(node, Strings.splitStringToArray(actionMasks, ','));
            ((MockTaskManager)clusterService.getTaskManager()).addListener(listener);
            RecordingTaskManagerListener oldListener = listeners.put(new Tuple<>(node.name(), actionMasks), listener);
            assertNull(oldListener);
        }
    }

    /**
     * Resets all recording task event listeners with the given action mask on all nodes
     */
    private void resetTaskManageListeners(String actionMasks) {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                entry.getValue().reset();
            }
        }
    }

    /**
     * Returns the number of events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return number of events that satisfy the criteria
     */
    private int numberOfEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        return findEvents(actionMasks, criteria).size();
    }

    /**
     * Returns all events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return number of events that satisfy the criteria
     */
    private List<TaskInfo> findEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        List<TaskInfo> events = new ArrayList<>();
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                for (Tuple<Boolean, TaskInfo> taskEvent : entry.getValue().getEvents()) {
                    if (criteria.apply(taskEvent)) {
                        events.add(taskEvent.v2());
                    }
                }
            }
        }
        return events;
    }

    /**
     * Asserts that all tasks in the tasks list have the same parentTask
     */
    private void assertParentTask(List<TaskInfo> tasks, TaskInfo parentTask) {
        for (TaskInfo task : tasks) {
            assertTrue(task.getParentTaskId().isSet());
            assertEquals(parentTask.getNode().getId(), task.getParentTaskId().getNodeId());
            assertTrue(Strings.hasLength(task.getParentTaskId().getNodeId()));
            assertEquals(parentTask.getId(), task.getParentTaskId().getId());
        }
    }
}
