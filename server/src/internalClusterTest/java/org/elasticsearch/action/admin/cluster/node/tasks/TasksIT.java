/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationActionTests;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.tasks.MockTaskManagerListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.elasticsearch.action.admin.cluster.node.tasks.TestTaskPlugin.TEST_TASK_ACTION;
import static org.elasticsearch.action.admin.cluster.node.tasks.TestTaskPlugin.UNBLOCK_TASK_ACTION;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration tests for task management API
 * <p>
 * We need at least 2 nodes so we have a master node a non-master node
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class TasksIT extends ESIntegTestCase {

    private final Map<Tuple<String, String>, RecordingTaskManagerListener> listeners = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = new ArrayList<>(super.getMockPlugins());
        mockPlugins.remove(MockTransportService.TestPlugin.class);
        return mockPlugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, TestTaskPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.getKey(), true)
            .build();
    }

    public void testTaskCounts() {
        // Run only on data nodes
        ListTasksResponse response = clusterAdmin().prepareListTasks("data:true")
            .setActions(TransportListTasksAction.TYPE.name() + "[n]")
            .get();
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(cluster().numDataNodes()));
    }

    public void testMasterNodeOperationTasks() throws Exception {
        registerTaskManagerListeners(TransportClusterHealthAction.NAME);

        // First run the health on the master node - should produce only one task on the master node
        internalCluster().masterClient().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
        assertEquals(1, numberOfEvents(TransportClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        // counting only unregistration events
        // When checking unregistration events there might be some delay since receiving the response from the cluster doesn't
        // guarantee that the task has been unregistered.
        assertBusy(() -> assertEquals(1, numberOfEvents(TransportClusterHealthAction.NAME, event -> event.v1() == false)));

        resetTaskManagerListeners(TransportClusterHealthAction.NAME);

        // Now run the health on a non-master node - should produce one task on master and one task on another node
        internalCluster().nonMasterClient().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
        assertEquals(2, numberOfEvents(TransportClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        // counting only unregistration events
        assertBusy(() -> assertEquals(2, numberOfEvents(TransportClusterHealthAction.NAME, event -> event.v1() == false)));
        List<TaskInfo> tasks = findEvents(TransportClusterHealthAction.NAME, Tuple::v1);

        // Verify that one of these tasks is a parent of another task
        if (tasks.get(0).parentTaskId().isSet()) {
            assertParentTask(Collections.singletonList(tasks.get(0)), tasks.get(1));
        } else {
            assertParentTask(Collections.singletonList(tasks.get(1)), tasks.get(0));
        }
    }

    public void testTransportReplicationAllShardsTasks() {
        registerTaskManagerListeners(ValidateQueryAction.NAME); // main task
        registerTaskManagerListeners(ValidateQueryAction.NAME + "[s]"); // shard
                                                                        // level
        // tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        indicesAdmin().prepareValidateQuery("test").setAllShards(true).get();

        // the field stats operation should produce one main task
        NumShards numberOfShards = getNumShards("test");
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, Tuple::v1));
        // and then one operation per shard
        assertEquals(numberOfShards.numPrimaries, numberOfEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1));

        // the shard level tasks should have the main task as a parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1), findEvents(ValidateQueryAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportBroadcastByNodeTasks() {
        registerTaskManagerListeners(ForceMergeAction.NAME);  // main task
        registerTaskManagerListeners(ForceMergeAction.NAME + "[n]"); // node level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        indicesAdmin().prepareForceMerge("test").get();

        // the percolate operation should produce one main task
        assertEquals(1, numberOfEvents(ForceMergeAction.NAME, Tuple::v1));
        // and then one operation per each node where shards are located
        assertEquals(internalCluster().nodesInclude("test").size(), numberOfEvents(ForceMergeAction.NAME + "[n]", Tuple::v1));

        // all node level tasks should have the main task as a parent
        assertParentTask(findEvents(ForceMergeAction.NAME + "[n]", Tuple::v1), findEvents(ForceMergeAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportReplicationSingleShardTasks() {
        registerTaskManagerListeners(ValidateQueryAction.NAME);  // main task
        registerTaskManagerListeners(ValidateQueryAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        indicesAdmin().prepareValidateQuery("test").get();

        // the validate operation should produce one main task
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, Tuple::v1));
        // and then one operation
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1));
        // the shard level operation should have the main task as its parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1), findEvents(ValidateQueryAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportBroadcastReplicationTasks() {
        registerTaskManagerListeners(RefreshAction.NAME);  // main task
        registerTaskManagerListeners(RefreshAction.NAME + "[s]"); // shard level tasks
        registerTaskManagerListeners(RefreshAction.NAME + "[s][*]"); // primary and replica shard tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        indicesAdmin().prepareRefresh("test").get();

        // the refresh operation should produce one main task
        NumShards numberOfShards = getNumShards("test");

        logger.debug("number of shards, total: [{}], primaries: [{}] ", numberOfShards.totalNumShards, numberOfShards.numPrimaries);
        logger.debug("main events {}", numberOfEvents(RefreshAction.NAME, Tuple::v1));
        logger.debug("main event node {}", findEvents(RefreshAction.NAME, Tuple::v1).get(0).taskId().getNodeId());
        logger.debug("[s] events {}", numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1));
        logger.debug("[s][*] events {}", numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));
        logger.debug("nodes with the index {}", internalCluster().nodesInclude("test"));

        assertEquals(1, numberOfEvents(RefreshAction.NAME, Tuple::v1));
        // Because it's broadcast replication action we will have as many [s] level requests
        // as we have primary shards on the coordinating node plus we will have one task per primary outside of the
        // coordinating node due to replication.
        // If all primaries are on the coordinating node, the number of tasks should be equal to the number of primaries
        // If all primaries are not on the coordinating node, the number of tasks should be equal to the number of primaries times 2
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), greaterThanOrEqualTo(numberOfShards.numPrimaries));
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), lessThanOrEqualTo(numberOfShards.numPrimaries * 2));

        // Verify that all [s] events have the proper parent
        // This is complicated because if the shard task runs on the same node it has main task as a parent
        // but if it runs on non-coordinating node it would have another intermediate [s] task on the coordinating node as a parent
        TaskInfo mainTask = findEvents(RefreshAction.NAME, Tuple::v1).get(0);
        List<TaskInfo> sTasks = findEvents(RefreshAction.NAME + "[s]", Tuple::v1);
        for (TaskInfo taskInfo : sTasks) {
            if (mainTask.taskId().getNodeId().equals(taskInfo.taskId().getNodeId())) {
                // This shard level task runs on the same node as a parent task - it should have the main task as a direct parent
                assertParentTask(Collections.singletonList(taskInfo), mainTask);
            } else {
                String description = taskInfo.description();
                // This shard level task runs on another node - it should have a corresponding shard level task on the node where main task
                // is running
                List<TaskInfo> sTasksOnRequestingNode = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1()
                        && mainTask.taskId().getNodeId().equals(event.v2().taskId().getNodeId())
                        && description.equals(event.v2().description())
                );
                // There should be only one parent task
                assertEquals(1, sTasksOnRequestingNode.size());
                assertParentTask(Collections.singletonList(taskInfo), sTasksOnRequestingNode.get(0));
            }
        }

        // we will have as many [s][p] and [s][r] tasks as we have primary and replica shards
        assertEquals(numberOfShards.totalNumShards, numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));

        // the [s][p] and [s][r] tasks should have a corresponding [s] task on the same node as a parent
        List<TaskInfo> spEvents = findEvents(RefreshAction.NAME + "[s][*]", Tuple::v1);
        for (TaskInfo taskInfo : spEvents) {
            List<TaskInfo> sTask;
            if (taskInfo.action().endsWith("[s][p]")) {
                // A [s][p] level task should have a corresponding [s] level task on the same node
                sTask = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1() // task registration event
                        && event.v2().taskId().equals(taskInfo.parentTaskId())
                        && event.v2().taskId().getNodeId().equals(taskInfo.taskId().getNodeId())
                );
            } else {
                // A [s][r] level task should have a corresponding [s] level task on a different node (where primary is located)
                sTask = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1() // task registration event
                        && event.v2().taskId().equals(taskInfo.parentTaskId())
                        && event.v2().taskId().getNodeId().equals(taskInfo.taskId().getNodeId()) == false
                );
            }
            // There should be only one parent task
            assertEquals(1, sTask.size());
            assertParentTask(Collections.singletonList(taskInfo), sTask.get(0));
        }
    }

    public void testTransportBulkTasks() {
        registerTaskManagerListeners(TransportBulkAction.NAME);  // main task
        registerTaskManagerListeners(TransportBulkAction.NAME + "[s]");  // shard task
        registerTaskManagerListeners(TransportBulkAction.NAME + "[s][p]");  // shard task on primary
        registerTaskManagerListeners(TransportBulkAction.NAME + "[s][r]");  // shard task on replica
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated to catch replication tasks
        // ensures the mapping is available on all nodes so we won't retry the request (in case replicas don't have the right mapping).
        indicesAdmin().preparePutMapping("test").setSource("foo", "type=keyword").get();
        client().prepareBulk().add(prepareIndex("test").setId("test_id").setSource("{\"foo\": \"bar\"}", XContentType.JSON)).get();

        // the bulk operation should produce one main task
        List<TaskInfo> topTask = findEvents(TransportBulkAction.NAME, Tuple::v1);
        assertEquals(1, topTask.size());
        assertEquals("requests[1], indices[test]", topTask.get(0).description());

        // we should also get 1 or 2 [s] operation with main operation as a parent
        // in case the primary is located on the coordinating node we will have 1 operation, otherwise - 2
        List<TaskInfo> shardTasks = findEvents(TransportBulkAction.NAME + "[s]", Tuple::v1);
        assertThat(shardTasks.size(), allOf(lessThanOrEqualTo(2), greaterThanOrEqualTo(1)));

        // Select the effective shard task
        TaskInfo shardTask;
        if (shardTasks.size() == 1) {
            // we have only one task - it's going to be the parent task for all [s][p] and [s][r] tasks
            shardTask = shardTasks.get(0);
            // and it should have the main task as a parent
            assertParentTask(shardTask, findEvents(TransportBulkAction.NAME, Tuple::v1).get(0));
        } else {
            if (shardTasks.get(0).parentTaskId().equals(shardTasks.get(1).taskId())) {
                // task 1 is the parent of task 0, that means that task 0 will control [s][p] and [s][r] tasks
                shardTask = shardTasks.get(0);
                // in turn the parent of the task 1 should be the main task
                assertParentTask(shardTasks.get(1), findEvents(TransportBulkAction.NAME, Tuple::v1).get(0));
            } else {
                // otherwise task 1 will control [s][p] and [s][r] tasks
                shardTask = shardTasks.get(1);
                // in turn the parent of the task 0 should be the main task
                assertParentTask(shardTasks.get(0), findEvents(TransportBulkAction.NAME, Tuple::v1).get(0));
            }
        }
        assertThat(shardTask.description(), startsWith("requests[1], index[test]["));

        // we should also get one [s][p] operation with shard operation as a parent
        assertEquals(1, numberOfEvents(TransportBulkAction.NAME + "[s][p]", Tuple::v1));
        assertParentTask(findEvents(TransportBulkAction.NAME + "[s][p]", Tuple::v1), shardTask);

        // we should get as many [s][r] operations as we have replica shards
        // they all should have the same shard task as a parent
        assertEquals(getNumShards("test").numReplicas, numberOfEvents(TransportBulkAction.NAME + "[s][r]", Tuple::v1));
        assertParentTask(findEvents(TransportBulkAction.NAME + "[s][r]", Tuple::v1), shardTask);
    }

    public void testSearchTaskDescriptions() {
        // TODO: enhance this test to also check the tasks created by batched query execution
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), false));
        registerTaskManagerListeners(TransportSearchAction.TYPE.name());  // main task
        registerTaskManagerListeners(TransportSearchAction.TYPE.name() + "[*]");  // shard task
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated to catch replication tasks
        prepareIndex("test").setId("test_id")
            .setSource("{\"foo\": \"bar\"}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Map<String, String> headers = new HashMap<>();
        headers.put(Task.X_OPAQUE_ID_HTTP_HEADER, "my_id");
        headers.put("Foo-Header", "bar");
        headers.put("Custom-Task-Header", "my_value");
        assertNoFailures(client().filterWithHeader(headers).prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()));

        // the search operation should produce one main task
        List<TaskInfo> mainTask = findEvents(TransportSearchAction.TYPE.name(), Tuple::v1);
        assertEquals(1, mainTask.size());
        assertThat(mainTask.get(0).description(), startsWith("indices[test], search_type["));
        assertThat(mainTask.get(0).description(), containsString("\"query\":{\"match_all\""));
        assertTaskHeaders(mainTask.get(0));

        // check that if we have any shard-level requests they all have non-zero length description
        List<TaskInfo> shardTasks = findEvents(TransportSearchAction.TYPE.name() + "[*]", Tuple::v1);
        for (TaskInfo taskInfo : shardTasks) {
            assertThat(taskInfo.parentTaskId(), notNullValue());
            assertEquals(mainTask.get(0).taskId(), taskInfo.parentTaskId());
            assertTaskHeaders(taskInfo);
            switch (taskInfo.action()) {
                case SearchTransportService.QUERY_ACTION_NAME, SearchTransportService.DFS_ACTION_NAME -> assertTrue(
                    taskInfo.description(),
                    Regex.simpleMatch("shardId[[test][*]]", taskInfo.description())
                );
                case SearchTransportService.QUERY_ID_ACTION_NAME -> assertTrue(
                    taskInfo.description(),
                    Regex.simpleMatch("id[*], indices[test]", taskInfo.description())
                );
                case SearchTransportService.FETCH_ID_ACTION_NAME -> assertTrue(
                    taskInfo.description(),
                    Regex.simpleMatch("id[*], size[1], lastEmittedDoc[null]", taskInfo.description())
                );
                default -> fail("Unexpected action [" + taskInfo.action() + "] with description [" + taskInfo.description() + "]");
            }
            // assert that all task descriptions have non-zero length
            assertThat(taskInfo.description().length(), greaterThan(0));
        }
        updateClusterSettings(Settings.builder().putNull(SearchService.BATCHED_QUERY_PHASE.getKey()));
    }

    public void testSearchTaskHeaderLimit() {
        int maxSize = Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.getDefault(Settings.EMPTY).getBytes() / 2 + 1);

        Map<String, String> headers = new HashMap<>();
        headers.put(Task.X_OPAQUE_ID_HTTP_HEADER, "my_id");
        headers.put("Custom-Task-Header", randomAlphaOfLengthBetween(maxSize, maxSize + 100));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            client().filterWithHeader(headers).admin().cluster().prepareListTasks()
        );
        assertThat(ex.getMessage(), startsWith("Request exceeded the maximum size of task headers "));
    }

    private void assertTaskHeaders(TaskInfo taskInfo) {
        assertThat(taskInfo.headers().keySet(), hasSize(2));
        assertEquals("my_id", taskInfo.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER));
        assertEquals("my_value", taskInfo.headers().get("Custom-Task-Header"));
    }

    /**
     * Very basic "is it plugged in" style test that indexes a document and makes sure that you can fetch the status of the process. The
     * goal here is to verify that the large moving parts that make fetching task status work fit together rather than to verify any
     * particular status results from indexing. For that, look at {@link TransportReplicationActionTests}. We intentionally don't use the
     * task recording mechanism used in other places in this test so we can make sure that the status fetching works properly over the wire.
     */
    public void testCanFetchIndexStatus() throws Exception {
        // First latch waits for the task to start, second on blocks it from finishing.
        CountDownLatch taskRegistered = new CountDownLatch(1);
        CountDownLatch letTaskFinish = new CountDownLatch(1);
        Thread index = null;
        try {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                ((MockTaskManager) transportService.getTaskManager()).addListener(new MockTaskManagerListener() {
                    @Override
                    public void onTaskRegistered(Task task) {
                        if (task.getAction().startsWith(TransportIndexAction.NAME)) {
                            taskRegistered.countDown();
                            logger.debug("Blocking [{}] starting", task);
                            try {
                                assertTrue(letTaskFinish.await(10, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                });
            }
            // Need to run the task in a separate thread because node client's .execute() is blocked by our task listener
            index = new Thread(() -> {
                DocWriteResponse indexResponse = prepareIndex("test").setSource("test", "test").get();
                assertArrayEquals(ReplicationResponse.NO_FAILURES, indexResponse.getShardInfo().getFailures());
            });
            index.start();
            assertTrue(taskRegistered.await(10, TimeUnit.SECONDS)); // waiting for at least one task to be registered

            ListTasksResponse listResponse = clusterAdmin().prepareListTasks()
                .setActions("indices:data/write/index*")
                .setDetailed(true)
                .get();
            assertThat(listResponse.getTasks(), not(empty()));
            for (TaskInfo task : listResponse.getTasks()) {
                assertNotNull(task.status());
                GetTaskResponse getResponse = clusterAdmin().prepareGetTask(task.taskId()).get();
                assertFalse("task should still be running", getResponse.getTask().isCompleted());
                TaskInfo fetchedWithGet = getResponse.getTask().getTask();
                assertEquals(task.id(), fetchedWithGet.id());
                assertEquals(task.type(), fetchedWithGet.type());
                assertEquals(task.action(), fetchedWithGet.action());
                assertEquals(task.description(), fetchedWithGet.description());
                assertEquals(task.status(), fetchedWithGet.status());
                assertEquals(task.startTime(), fetchedWithGet.startTime());
                assertThat(fetchedWithGet.runningTimeNanos(), greaterThanOrEqualTo(task.runningTimeNanos()));
                assertEquals(task.cancellable(), fetchedWithGet.cancellable());
                assertEquals(task.parentTaskId(), fetchedWithGet.parentTaskId());
            }
        } finally {
            letTaskFinish.countDown();
            if (index != null) {
                index.join();
            }
            assertBusy(
                () -> {
                    assertEquals(emptyList(), clusterAdmin().prepareListTasks().setActions("indices:data/write/index*").get().getTasks());
                }
            );
        }
    }

    public void testTasksCancellation() throws Exception {
        // Start blocking test task
        // Get real client (the plugin is not registered on transport nodes)
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TEST_TASK_ACTION, request);

        logger.info("--> started test tasks");

        // Wait for the task to start on all nodes
        assertBusy(
            () -> assertEquals(
                internalCluster().size(),
                clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name() + "[n]").get().getTasks().size()
            )
        );

        logger.info("--> cancelling the main test task");
        ListTasksResponse cancelTasksResponse = clusterAdmin().prepareCancelTasks().setActions(TEST_TASK_ACTION.name()).get();
        assertEquals(1, cancelTasksResponse.getTasks().size());

        expectThrows(TaskCancelledException.class, future);

        logger.info("--> waiting for all ongoing tasks to complete within a reasonable time");
        safeGet(clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name() + "*").setWaitForCompletion(true).execute());

        logger.info("--> checking that test tasks are not running");
        assertEquals(0, clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name() + "*").get().getTasks().size());
    }

    public void testTasksUnblocking() throws Exception {
        // Start blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TEST_TASK_ACTION, request);
        // Wait for the task to start on all nodes
        assertBusy(
            () -> assertEquals(
                internalCluster().size(),
                clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name() + "[n]").get().getTasks().size()
            )
        );

        client().execute(UNBLOCK_TASK_ACTION, new TestTaskPlugin.UnblockTestTasksRequest()).get();

        future.get();
        assertBusy(
            () -> assertEquals(0, clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name() + "[n]").get().getTasks().size())
        );
    }

    public void testListTasksWaitForCompletion() throws Exception {
        waitForCompletionTestCase(randomBoolean(), id -> {
            var future = ensureStartedOnAllNodes(
                "cluster:monitor/tasks/lists[n]",
                () -> clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name()).setWaitForCompletion(true).execute()
            );

            // This ensures that a task has progressed to the point of listing all running tasks and subscribing to their updates
            for (var threadPool : internalCluster().getInstances(ThreadPool.class)) {
                flushThreadPoolExecutor(threadPool, ThreadPool.Names.MANAGEMENT);
            }

            return future;
        }, response -> {
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getTasks(), hasSize(1));
            TaskInfo task = response.getTasks().get(0);
            assertEquals(TEST_TASK_ACTION.name(), task.action());
        });
    }

    public void testGetTaskWaitForCompletionWithoutStoringResult() throws Exception {
        waitForCompletionTestCase(false, id -> clusterAdmin().prepareGetTask(id).setWaitForCompletion(true).execute(), response -> {
            assertTrue(response.getTask().isCompleted());
            // We didn't store the result so it won't come back when we wait
            assertNull(response.getTask().getResponse());
            // But the task's details should still be there because we grabbed a reference to the task before waiting for it to complete
            assertNotNull(response.getTask().getTask());
            assertEquals(TEST_TASK_ACTION.name(), response.getTask().getTask().action());
        });
    }

    public void testGetTaskWaitForCompletionWithStoringResult() throws Exception {
        waitForCompletionTestCase(true, id -> clusterAdmin().prepareGetTask(id).setWaitForCompletion(true).execute(), response -> {
            assertTrue(response.getTask().isCompleted());
            // We stored the task so we should get its results
            assertEquals(0, response.getTask().getResponseAsMap().get("failure_count"));
            // The task's details should also be there
            assertNotNull(response.getTask().getTask());
            assertEquals(TEST_TASK_ACTION.name(), response.getTask().getTask().action());
        });
    }

    /**
     * Test wait for completion.
     * @param storeResult should the task store its results
     * @param wait start waiting for a task. Accepts that id of the task to wait for and returns a future waiting for it.
     * @param validator validate the response and return the task ids that were found
     */
    private <T> void waitForCompletionTestCase(boolean storeResult, Function<TaskId, ActionFuture<T>> wait, Consumer<T> validator)
        throws Exception {
        // Start blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldStoreResult(storeResult);

        ActionFuture<TestTaskPlugin.NodesResponse> future = ensureStartedOnAllNodes(
            TEST_TASK_ACTION.name() + "[n]",
            () -> client().execute(TEST_TASK_ACTION, request)
        );
        var tasks = clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name()).get().getTasks();
        assertThat(tasks, hasSize(1));
        TaskId taskId = tasks.get(0).taskId();
        clusterAdmin().prepareGetTask(taskId).get();

        var taskManager = (MockTaskManager) internalCluster().getInstance(
            TransportService.class,
            clusterService().state().getNodes().resolveNode(taskId.getNodeId()).getName()
        ).getTaskManager();
        var listener = new MockTaskManagerListener() {
            @Override
            public void onRemovedTaskListenerRegistered(RemovedTaskListener removedTaskListener) {
                // Unblock the request only after it started waiting for task completion
                client().execute(UNBLOCK_TASK_ACTION, new TestTaskPlugin.UnblockTestTasksRequest());
            }
        };
        taskManager.addListener(listener);
        try {
            // Spin up a request to wait for the test task to finish
            // The task will be unblocked as soon as the request started waiting for task completion
            T waitResponse = wait.apply(taskId).get();
            validator.accept(waitResponse);
        } finally {
            taskManager.removeListener(listener);
        }

        TestTaskPlugin.NodesResponse response = future.get();
        assertEquals(emptyList(), response.failures());
    }

    public void testListTasksWaitForTimeout() throws Exception {
        waitForTimeoutTestCase(id -> {
            ListTasksResponse response = clusterAdmin().prepareListTasks()
                .setActions(TEST_TASK_ACTION.name())
                .setWaitForCompletion(true)
                .setTimeout(timeValueMillis(100))
                .get();
            assertThat(response.getNodeFailures(), not(empty()));
            return response.getNodeFailures();
        });
    }

    public void testGetTaskWaitForTimeout() throws Exception {
        waitForTimeoutTestCase(id -> {
            Exception e = expectThrows(
                Exception.class,
                clusterAdmin().prepareGetTask(id).setWaitForCompletion(true).setTimeout(timeValueMillis(100))
            );
            return singleton(e);
        });
    }

    /**
     * Test waiting for a task that times out.
     * @param wait wait for the running task and return all the failures you accumulated waiting for it
     */
    private void waitForTimeoutTestCase(Function<TaskId, ? extends Iterable<? extends Throwable>> wait) throws Exception {
        // Start blocking test task
        ActionFuture<TestTaskPlugin.NodesResponse> future = ensureStartedOnAllNodes(
            TEST_TASK_ACTION.name() + "[n]",
            () -> client().execute(TEST_TASK_ACTION, new TestTaskPlugin.NodesRequest("test"))
        );
        try {
            var tasks = clusterAdmin().prepareListTasks().setActions(TEST_TASK_ACTION.name()).get().getTasks();
            assertThat(tasks, hasSize(1));
            var taskId = tasks.get(0).taskId();
            clusterAdmin().prepareGetTask(taskId).get();
            // Spin up a request that should wait for those tasks to finish
            // It will timeout because we haven't unblocked the tasks
            Iterable<? extends Throwable> failures = wait.apply(taskId);

            for (Throwable failure : failures) {
                assertNotNull(
                    ExceptionsHelper.unwrap(failure, ElasticsearchTimeoutException.class, ReceiveTimeoutTransportException.class)
                );
            }
        } finally {
            // Now we can unblock those requests
            client().execute(UNBLOCK_TASK_ACTION, new TestTaskPlugin.UnblockTestTasksRequest()).get();
        }
        future.get();
    }

    private <T> ActionFuture<T> ensureStartedOnAllNodes(String nodeTaskName, Supplier<ActionFuture<T>> taskStarter) {
        var startedOnAllNodes = new CountDownLatch(internalCluster().size());
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTaskManager) transportService.getTaskManager()).addListener(new MockTaskManagerListener() {
                @Override
                public void onTaskRegistered(Task task) {
                    if (Objects.equals(task.getAction(), nodeTaskName)) {
                        startedOnAllNodes.countDown();
                    }
                }
            });
        }
        var future = taskStarter.get();
        safeAwait(startedOnAllNodes);
        return future;
    }

    public void testTasksListWaitForNoTask() throws Exception {
        // Spin up a request to wait for no matching tasks
        ActionFuture<ListTasksResponse> waitResponseFuture = clusterAdmin().prepareListTasks()
            .setActions(TEST_TASK_ACTION.name() + "[n]")
            .setWaitForCompletion(true)
            .setTimeout(timeValueMillis(10))
            .execute();

        // It should finish quickly and without complaint
        assertThat(waitResponseFuture.get().getTasks(), empty());
    }

    public void testTasksGetWaitForNoTask() throws Exception {
        // Spin up a request to wait for no matching tasks
        ActionFuture<GetTaskResponse> waitResponseFuture = clusterAdmin().prepareGetTask("notfound:1")
            .setWaitForCompletion(true)
            .setTimeout(timeValueMillis(10))
            .execute();

        // It should finish quickly and without complaint
        expectNotFound(waitResponseFuture::get);
    }

    public void testTasksWaitForAllTask() throws Exception {
        // Find tasks that are not expected to complete and identify the nodes running them
        List<PersistentTasksCustomMetadata.PersistentTask<?>> alwaysRunningTasks = findTasks(
            clusterService().state(),
            HealthNode.TASK_NAME
        );
        Set<String> nodesRunningTasks = alwaysRunningTasks.stream()
            .map(PersistentTasksCustomMetadata.PersistentTask::getExecutorNode)
            .collect(Collectors.toSet());
        // Spin up a request to wait for all tasks in the cluster to make sure it doesn't cause an infinite loop
        ListTasksResponse response = clusterAdmin().prepareListTasks().setWaitForCompletion(true).setTimeout(timeValueSeconds(1)).get();

        // We expect the nodes that are running always-running-tasks to report FailedNodeException and fail to list their tasks
        assertThat(response.getNodeFailures().size(), equalTo(nodesRunningTasks.size()));
        assertThat(
            response.getNodeFailures().stream().map(f -> ((FailedNodeException) f).nodeId()).collect(Collectors.toSet()),
            equalTo(nodesRunningTasks)
        );
        // We expect no task failures, at least one task the completed, the listTasks task on a node with completed tasks (1 out of min 2).
        assertThat(response.getTaskFailures(), emptyCollectionOf(TaskOperationFailure.class));
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(1));
    }

    public void testTaskStoringSuccessfulResult() throws Exception {
        registerTaskManagerListeners(TEST_TASK_ACTION.name());  // we need this to get task id of the process

        // Start non-blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldStoreResult(true);
        request.setShouldBlock(false);
        TaskId parentTaskId = new TaskId("parent_node", randomLong());
        request.setParentTask(parentTaskId);

        client().execute(TEST_TASK_ACTION, request).get();

        List<TaskInfo> events = findEvents(TEST_TASK_ACTION.name(), Tuple::v1);

        assertEquals(1, events.size());
        TaskInfo taskInfo = events.get(0);
        TaskId taskId = taskInfo.taskId();

        TaskResult taskResult = clusterAdmin().getTask(new GetTaskRequest().setTaskId(taskId)).get().getTask();
        assertTrue(taskResult.isCompleted());
        assertNull(taskResult.getError());

        assertEquals(taskInfo.taskId(), taskResult.getTask().taskId());
        assertEquals(taskInfo.parentTaskId(), taskResult.getTask().parentTaskId());
        assertEquals(taskInfo.type(), taskResult.getTask().type());
        assertEquals(taskInfo.action(), taskResult.getTask().action());
        assertEquals(taskInfo.description(), taskResult.getTask().description());
        assertEquals(taskInfo.startTime(), taskResult.getTask().startTime());
        assertEquals(taskInfo.headers(), taskResult.getTask().headers());
        Map<?, ?> result = taskResult.getResponseAsMap();
        assertEquals("0", result.get("failure_count").toString());

        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());

        assertHitCount(
            1L,
            prepareSearch(TaskResultsService.TASK_INDEX).setSource(
                SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("task.action", taskInfo.action()))
            ),
            prepareSearch(TaskResultsService.TASK_INDEX).setSource(
                SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("task.node", taskInfo.taskId().getNodeId()))
            )
        );

        GetTaskResponse getResponse = expectFinishedTask(taskId);
        assertEquals(result, getResponse.getTask().getResponseAsMap());
        assertNull(getResponse.getTask().getError());

        // run it again to check that the tasks index has been successfully created and can be re-used
        client().execute(TEST_TASK_ACTION, request).get();

        events = findEvents(TEST_TASK_ACTION.name(), Tuple::v1);

        assertEquals(2, events.size());
    }

    public void testTaskStoringFailureResult() throws Exception {
        registerTaskManagerListeners(TEST_TASK_ACTION.name());  // we need this to get task id of the process

        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldFail(true);
        request.setShouldStoreResult(true);
        request.setShouldBlock(false);

        // Start non-blocking test task that should fail
        assertFutureThrows(client().execute(TEST_TASK_ACTION, request), IllegalStateException.class);

        List<TaskInfo> events = findEvents(TEST_TASK_ACTION.name(), Tuple::v1);
        assertEquals(1, events.size());
        TaskInfo failedTaskInfo = events.get(0);
        TaskId failedTaskId = failedTaskInfo.taskId();

        TaskResult taskResult = clusterAdmin().getTask(new GetTaskRequest().setTaskId(failedTaskId)).get().getTask();
        assertTrue(taskResult.isCompleted());
        assertNull(taskResult.getResponse());

        assertEquals(failedTaskInfo.taskId(), taskResult.getTask().taskId());
        assertEquals(failedTaskInfo.type(), taskResult.getTask().type());
        assertEquals(failedTaskInfo.action(), taskResult.getTask().action());
        assertEquals(failedTaskInfo.description(), taskResult.getTask().description());
        assertEquals(failedTaskInfo.startTime(), taskResult.getTask().startTime());
        assertEquals(failedTaskInfo.headers(), taskResult.getTask().headers());
        Map<?, ?> error = (Map<?, ?>) taskResult.getErrorAsMap();
        assertEquals("Simulating operation failure", error.get("reason"));
        assertEquals("illegal_state_exception", error.get("type"));

        GetTaskResponse getResponse = expectFinishedTask(failedTaskId);
        assertNull(getResponse.getTask().getResponse());
        assertEquals(error, getResponse.getTask().getErrorAsMap());
    }

    public void testGetTaskNotFound() throws Exception {
        // Node isn't found, tasks index doesn't even exist
        expectNotFound(() -> clusterAdmin().prepareGetTask("not_a_node:1").get());

        // Node exists but the task still isn't found
        expectNotFound(() -> clusterAdmin().prepareGetTask(new TaskId(internalCluster().getNodeNames()[0], 1)).get());
    }

    public void testNodeNotFoundButTaskFound() throws Exception {
        // Save a fake task that looks like it is from a node that isn't part of the cluster
        CyclicBarrier b = new CyclicBarrier(2);
        TaskResultsService resultsService = internalCluster().getInstance(TaskResultsService.class);
        resultsService.storeResult(
            new TaskResult(
                new TaskInfo(
                    new TaskId("fake", 1),
                    "test",
                    "fake",
                    "test",
                    "",
                    null,
                    0,
                    0,
                    false,
                    false,
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap()
                ),
                new RuntimeException("test")
            ),
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void response) {
                    try {
                        b.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        b.await();

        // Now we can find it!
        GetTaskResponse response = expectFinishedTask(new TaskId("fake:1"));
        assertEquals("test", response.getTask().getTask().action());
        assertNotNull(response.getTask().getError());
        assertNull(response.getTask().getResponse());
    }

    @Override
    public void tearDown() throws Exception {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            ((MockTaskManager) internalCluster().getInstance(TransportService.class, entry.getKey().v1()).getTaskManager()).removeListener(
                entry.getValue()
            );
        }
        listeners.clear();
        super.tearDown();
    }

    /**
     * Registers recording task event listeners with the given action mask on all nodes
     */
    private void registerTaskManagerListeners(String actionMasks) {
        for (String nodeName : internalCluster().getNodeNames()) {
            DiscoveryNode node = internalCluster().getInstance(ClusterService.class, nodeName).localNode();
            RecordingTaskManagerListener listener = new RecordingTaskManagerListener(node.getId(), actionMasks.split(","));
            ((MockTaskManager) internalCluster().getInstance(TransportService.class, nodeName).getTaskManager()).addListener(listener);
            RecordingTaskManagerListener oldListener = listeners.put(new Tuple<>(node.getName(), actionMasks), listener);
            assertNull(oldListener);
        }
    }

    /**
     * Resets all recording task event listeners with the given action mask on all nodes
     */
    private void resetTaskManagerListeners(String actionMasks) {
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
            assertParentTask(task, parentTask);
        }
    }

    private void assertParentTask(TaskInfo task, TaskInfo parentTask) {
        assertTrue(task.parentTaskId().isSet());
        assertEquals(parentTask.taskId().getNodeId(), task.parentTaskId().getNodeId());
        assertTrue(Strings.hasLength(task.parentTaskId().getNodeId()));
        assertEquals(parentTask.id(), task.parentTaskId().getId());
    }

    private void expectNotFound(ThrowingRunnable r) {
        Exception e = expectThrows(Exception.class, r);
        ResourceNotFoundException notFound = (ResourceNotFoundException) ExceptionsHelper.unwrap(e, ResourceNotFoundException.class);
        if (notFound == null) {
            throw new AssertionError("Expected " + ResourceNotFoundException.class.getSimpleName(), e);
        }
    }

    /**
     * Fetch the task status from the list tasks API using it's "fallback to get from the task index" behavior. Asserts some obvious stuff
     * about the fetched task and returns a map of it's status.
     */
    private GetTaskResponse expectFinishedTask(TaskId taskId) throws IOException {
        GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).get();
        assertTrue("the task should have been completed before fetching", response.getTask().isCompleted());
        TaskInfo info = response.getTask().getTask();
        assertEquals(taskId, info.taskId());
        assertNull(info.status()); // The test task doesn't have any status
        return response;
    }
}
