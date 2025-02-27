/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService.WaitForPersistentTaskListener;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.State;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestTasksRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class PersistentTasksExecutorIT extends ESIntegTestCase {

    private static PersistentTasksExecutor.Scope scope;

    @BeforeClass
    public static void randomScope() {
        scope = randomFrom(PersistentTasksExecutor.Scope.values());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TestPersistentTasksPlugin.PERSISTENT_TASK_SCOPE_SETTING.getKey(), scope)
            .build();
    }

    @Before
    public void resetNonClusterStateCondition() {
        TestPersistentTasksExecutor.setNonClusterStateCondition(true);
    }

    @After
    public void cleanup() throws Exception {
        assertNoRunningTasks();
    }

    public static class WaitForPersistentTaskFuture<Params extends PersistentTaskParams> extends PlainActionFuture<PersistentTask<Params>>
        implements
            WaitForPersistentTaskListener<Params> {}

    public void testPersistentActionFailure() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future
        );
        long allocationId = future.get().getAllocationId();
        waitForTaskToStart();
        TaskInfo firstRunningTask = clusterAdmin().prepareListTasks()
            .setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get()
            .getTasks()
            .get(0);
        logger.info("Found running task with id {} and parent {}", firstRunningTask.id(), firstRunningTask.parentTaskId());
        // Verifying parent
        assertThat(firstRunningTask.parentTaskId().getId(), equalTo(allocationId));
        assertThat(firstRunningTask.parentTaskId().getNodeId(), equalTo("cluster"));

        logger.info("Failing the running task");
        // Fail the running task and make sure it restarts properly
        assertThat(
            new TestTasksRequestBuilder(client()).setOperation("fail").setTargetTaskId(firstRunningTask.taskId()).get().getTasks().size(),
            equalTo(1)
        );

        logger.info("Waiting for persistent task with id {} to disappear", firstRunningTask.id());
        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(), empty());
        });
    }

    public void testPersistentActionCompletion() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.sendStartRequest(
            taskId,
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future
        );
        long allocationId = future.get().getAllocationId();
        waitForTaskToStart();
        TaskInfo firstRunningTask = clusterAdmin().prepareListTasks()
            .setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .setDetailed(true)
            .get()
            .getTasks()
            .get(0);
        logger.info("Found running task with id {} and parent {}", firstRunningTask.id(), firstRunningTask.parentTaskId());
        // Verifying parent and description
        assertThat(firstRunningTask.parentTaskId().getId(), equalTo(allocationId));
        assertThat(firstRunningTask.parentTaskId().getNodeId(), equalTo("cluster"));
        assertThat(firstRunningTask.description(), equalTo("id=" + taskId));

        if (randomBoolean()) {
            logger.info("Simulating errant completion notification");
            // try sending completion request with incorrect allocation id
            PlainActionFuture<PersistentTask<?>> failedCompletionNotificationFuture = new PlainActionFuture<>();
            persistentTasksService.sendCompletionRequest(
                taskId,
                Long.MAX_VALUE,
                null,
                null,
                TEST_REQUEST_TIMEOUT,
                failedCompletionNotificationFuture
            );
            assertFutureThrows(failedCompletionNotificationFuture, ResourceNotFoundException.class);
            // Make sure that the task is still running
            assertThat(
                clusterAdmin().prepareListTasks()
                    .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .size(),
                equalTo(1)
            );
        }

        stopOrCancelTask(firstRunningTask.taskId());
    }

    public void testPersistentActionWithNoAvailableNode() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        testParams.setExecutorNodeAttr("test");
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            testParams,
            TEST_REQUEST_TIMEOUT,
            future
        );
        String taskId = future.get().getId();

        Settings nodeSettings = Settings.builder().put(nodeSettings(0, Settings.EMPTY)).put("node.attr.test_attr", "test").build();
        String newNode = internalCluster().startNode(nodeSettings);
        String newNodeId = getNodeId(newNode);
        waitForTaskToStart();

        TaskInfo taskInfo = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().get(0);

        // Verifying the task runs on the new node
        assertThat(taskInfo.taskId().getNodeId(), equalTo(newNodeId));

        internalCluster().stopNode(
            internalCluster().getNodeNameThat(settings -> Objects.equals(settings.get("node.attr.test_attr"), "test"))
        );

        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(), empty());
        });

        // Remove the persistent task
        PlainActionFuture<PersistentTask<?>> removeFuture = new PlainActionFuture<>();
        persistentTasksService.sendRemoveRequest(taskId, TEST_REQUEST_TIMEOUT, removeFuture);
        assertEquals(removeFuture.get().getId(), taskId);
    }

    public void testPersistentActionWithNonClusterStateCondition() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(1));

        TestPersistentTasksExecutor.setNonClusterStateCondition(false);

        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            testParams,
            TEST_REQUEST_TIMEOUT,
            future
        );
        String taskId = future.get().getId();

        assertThat(clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(), empty());

        TestPersistentTasksExecutor.setNonClusterStateCondition(true);

        waitForTaskToStart();
        TaskInfo taskInfo = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().get(0);

        // Verifying the task can now be assigned
        assertThat(taskInfo.taskId().getNodeId(), notNullValue());

        // Remove the persistent task
        PlainActionFuture<PersistentTask<?>> removeFuture = new PlainActionFuture<>();
        persistentTasksService.sendRemoveRequest(taskId, TEST_REQUEST_TIMEOUT, removeFuture);
        assertEquals(removeFuture.get().getId(), taskId);
    }

    public void testPersistentActionStatusUpdate() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future
        );
        String taskId = future.get().getId();
        waitForTaskToStart();
        TaskInfo firstRunningTask = clusterAdmin().prepareListTasks()
            .setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get()
            .getTasks()
            .get(0);

        List<PersistentTask<?>> tasksInProgress = findTasks(internalCluster().clusterService().state(), TestPersistentTasksExecutor.NAME);
        assertThat(tasksInProgress.size(), equalTo(1));
        assertThat(tasksInProgress.iterator().next().getState(), nullValue());

        int numberOfUpdates = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfUpdates; i++) {
            logger.info("Updating the task states");
            // Complete the running task and make sure it finishes properly
            assertThat(
                new TestTasksRequestBuilder(client()).setOperation("update_status")
                    .setTargetTaskId(firstRunningTask.taskId())
                    .get()
                    .getTasks()
                    .size(),
                equalTo(1)
            );

            int finalI = i;
            WaitForPersistentTaskFuture<?> future1 = new WaitForPersistentTaskFuture<>();
            waitForPersistentTaskCondition(
                persistentTasksService,
                taskId,
                task -> task != null
                    && task.getState() != null
                    && task.getState().toString() != null
                    && task.getState().toString().equals("{\"phase\":\"phase " + (finalI + 1) + "\"}"),
                TimeValue.timeValueSeconds(10),
                future1
            );
            assertThat(future1.get().getId(), equalTo(taskId));
        }

        WaitForPersistentTaskFuture<?> future1 = new WaitForPersistentTaskFuture<>();
        waitForPersistentTaskCondition(persistentTasksService, taskId, task -> false, TimeValue.timeValueMillis(10), future1);

        assertFutureThrows(future1, IllegalStateException.class, "timed out after 10ms");

        PlainActionFuture<PersistentTask<?>> failedUpdateFuture = new PlainActionFuture<>();
        persistentTasksService.sendUpdateStateRequest(taskId, -2, new State("should fail"), TEST_REQUEST_TIMEOUT, failedUpdateFuture);
        assertFutureThrows(
            failedUpdateFuture,
            ResourceNotFoundException.class,
            "the task with id " + taskId + " and allocation id -2 doesn't exist"
        );

        // Wait for the task to disappear
        WaitForPersistentTaskFuture<?> future2 = new WaitForPersistentTaskFuture<>();
        waitForPersistentTaskCondition(persistentTasksService, taskId, Objects::isNull, TimeValue.timeValueSeconds(10), future2);

        logger.info("Completing the running task");
        // Complete the running task and make sure it finishes properly
        assertThat(
            new TestTasksRequestBuilder(client()).setOperation("finish").setTargetTaskId(firstRunningTask.taskId()).get().getTasks().size(),
            equalTo(1)
        );

        assertThat(future2.get(), nullValue());
    }

    public void testCreatePersistentTaskWithDuplicateId() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.sendStartRequest(
            taskId,
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future
        );
        future.get();

        PlainActionFuture<PersistentTask<TestParams>> future2 = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            taskId,
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future2
        );
        assertFutureThrows(future2, ResourceAlreadyExistsException.class);

        waitForTaskToStart();

        TaskInfo firstRunningTask = clusterAdmin().prepareListTasks()
            .setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get()
            .getTasks()
            .get(0);

        logger.info("Completing the running task");
        // Fail the running task and make sure it restarts properly
        assertThat(
            new TestTasksRequestBuilder(client()).setOperation("finish").setTargetTaskId(firstRunningTask.taskId()).get().getTasks().size(),
            equalTo(1)
        );

        logger.info("Waiting for persistent task with id {} to disappear", firstRunningTask.id());
        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(), empty());
        });
    }

    public void testUnassignRunningPersistentTask() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(1));
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        testParams.setExecutorNodeAttr("test");
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            testParams,
            TEST_REQUEST_TIMEOUT,
            future
        );
        PersistentTask<TestParams> task = future.get();
        String taskId = task.getId();

        Settings nodeSettings = Settings.builder().put(nodeSettings(0, Settings.EMPTY)).put("node.attr.test_attr", "test").build();
        internalCluster().startNode(nodeSettings);

        waitForTaskToStart();

        PlainActionFuture<PersistentTask<?>> unassignmentFuture = new PlainActionFuture<>();

        // Disallow re-assignment after it is unassigned to verify master and node state
        TestPersistentTasksExecutor.setNonClusterStateCondition(false);

        persistentTasksClusterService.unassignPersistentTask(
            Metadata.DEFAULT_PROJECT_ID,
            taskId,
            task.getAllocationId() + 1,
            "unassignment test",
            unassignmentFuture
        );
        PersistentTask<?> unassignedTask = unassignmentFuture.get();
        assertThat(unassignedTask.getId(), equalTo(taskId));
        assertThat(unassignedTask.getAssignment().getExplanation(), equalTo("unassignment test"));
        assertThat(unassignedTask.getAssignment().getExecutorNode(), is(nullValue()));

        assertBusy(() -> {
            // Verify that the task is NOT running on the node
            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks();
            assertThat(tasks.size(), equalTo(0));

            // Verify that the task is STILL in internal cluster state
            assertClusterStateHasTask(taskId);
        });

        // Allow it to be reassigned again to the same node
        TestPersistentTasksExecutor.setNonClusterStateCondition(true);

        // Verify it starts again
        waitForTaskToStart();

        assertClusterStateHasTask(taskId);

        // Complete or cancel the running task
        TaskInfo taskInfo = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().get(0);
        stopOrCancelTask(taskInfo.taskId());
    }

    public void testAbortLocally() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(1));
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            TestPersistentTasksExecutor.NAME,
            new TestParams("Blah"),
            TEST_REQUEST_TIMEOUT,
            future
        );
        String taskId = future.get().getId();
        long allocationId = future.get().getAllocationId();
        waitForTaskToStart();
        TaskInfo firstRunningTask = clusterAdmin().prepareListTasks()
            .setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get()
            .getTasks()
            .get(0);

        // Disallow re-assignment after it is unassigned to verify master and node state
        TestPersistentTasksExecutor.setNonClusterStateCondition(false);

        // Verifying parent
        assertThat(firstRunningTask.parentTaskId().getId(), equalTo(allocationId));
        assertThat(firstRunningTask.parentTaskId().getNodeId(), equalTo("cluster"));

        assertThat(
            new TestTasksRequestBuilder(client()).setOperation("abort_locally")
                .setTargetTaskId(firstRunningTask.taskId())
                .get()
                .getTasks()
                .size(),
            equalTo(1)
        );

        assertBusy(() -> {
            // Verify that the task is NOT running on any node
            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks();
            assertThat(tasks.size(), equalTo(0));

            // Verify that the task is STILL in internal cluster state, unassigned, with a reason indicating local abort
            PersistentTask<?> task = assertClusterStateHasTask(taskId);
            assertThat(task.getAssignment().getExecutorNode(), nullValue());
            // Although the assignment explanation is initially set to "Simulating local abort", because
            // of the way we prevent reassignment to the same node in this test it may quickly change to
            // "non cluster state condition prevents assignment" - either proves the unassignment worked
            assertThat(
                task.getAssignment().getExplanation(),
                either(equalTo("Simulating local abort")).or(equalTo("non cluster state condition prevents assignment"))
            );
        });

        // Allow it to be reassigned again
        TestPersistentTasksExecutor.setNonClusterStateCondition(true);

        // Verify it starts again
        waitForTaskToStart();

        // Verify that persistent task is in cluster state and that the local abort reason has been removed.
        // (Since waitForTaskToStart() waited for the local task to start, there might be a short period when
        // the tasks API reports the local task but the cluster state update containing the new assignment
        // reason has not been published, hence the busy wait here.)
        assertBusy(() -> {
            PersistentTask<?> task = assertClusterStateHasTask(taskId);
            assertThat(task.getAssignment().getExplanation(), not(equalTo("Simulating local abort")));
        });

        // Complete or cancel the running task
        TaskInfo taskInfo = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().get(0);
        stopOrCancelTask(taskInfo.taskId());
    }

    private void waitForPersistentTaskCondition(
        PersistentTasksService persistentTasksService,
        String taskId,
        Predicate<PersistentTask<?>> predicate,
        @Nullable TimeValue timeout,
        WaitForPersistentTaskListener<?> listener
    ) throws Exception {
        if (scope == PersistentTasksExecutor.Scope.CLUSTER) {
            @FixForMultiProject(description = "can be replaced if PersistentTasksService supports waiting for cluster task conditions")
            final var clusterService = persistentTasksService.getClusterService();
            final var threadPool = persistentTasksService.getThreadPool();
            ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(ClusterPersistentTasksCustomMetadata.getTaskWithId(state, taskId));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, clusterState -> predicate.test(ClusterPersistentTasksCustomMetadata.getTaskWithId(clusterState, taskId)), timeout, logger);
        } else {
            persistentTasksService.waitForPersistentTaskCondition(taskId, predicate, timeout, listener);
        }
    }

    private void stopOrCancelTask(TaskId taskId) {
        if (randomBoolean()) {
            logger.info("Completing the running task");
            // Complete the running task and make sure it finishes properly
            assertThat(
                new TestTasksRequestBuilder(client()).setOperation("finish").setTargetTaskId(taskId).get().getTasks().size(),
                equalTo(1)
            );

        } else {
            logger.info("Cancelling the running task");
            // Cancel the running task and make sure it finishes properly
            assertThat(clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId).get().getTasks().size(), equalTo(1));
        }
    }

    private static void waitForTaskToStart() throws Exception {
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(
                clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().size(),
                equalTo(1)
            );
        });
    }

    private static PersistentTask<?> assertClusterStateHasTask(String taskId) {
        ClusterState state = internalCluster().clusterService().state();
        Collection<PersistentTask<?>> clusterTasks = findTasks(state, TestPersistentTasksExecutor.NAME);
        assertThat(clusterTasks, hasSize(1));
        PersistentTask<?> task = clusterTasks.iterator().next();
        assertThat(task.getId(), equalTo(taskId));
        return task;
    }

    private void assertNoRunningTasks() throws Exception {
        assertBusy(() -> {
            // Wait for the task to finish
            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks();
            logger.info("Found {} tasks", tasks.size());
            assertThat(tasks.size(), equalTo(0));

            // Make sure the task is removed from the cluster state
            ClusterState state = internalCluster().clusterService().state();
            assertThat(findTasks(state, TestPersistentTasksExecutor.NAME), empty());
        });
    }

}
