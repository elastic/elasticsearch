/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksService.WaitForPersistentTaskStatusListener;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.Status;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestTasksRequestBuilder;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class PersistentTasksExecutorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @After
    public void cleanup() throws Exception {
        assertNoRunningTasks();
    }

    public static class WaitForPersistentTaskStatusFuture<Params extends PersistentTaskParams>
            extends PlainActionFuture<PersistentTask<Params>>
            implements WaitForPersistentTaskStatusListener<Params> {
    }

    public void testPersistentActionFailure() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.startPersistentTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        long allocationId = future.get().getAllocationId();
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks().size(), equalTo(1));
        });
        TaskInfo firstRunningTask = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);
        logger.info("Found running task with id {} and parent {}", firstRunningTask.getId(), firstRunningTask.getParentTaskId());
        // Verifying parent
        assertThat(firstRunningTask.getParentTaskId().getId(), equalTo(allocationId));
        assertThat(firstRunningTask.getParentTaskId().getNodeId(), equalTo("cluster"));

        logger.info("Failing the running task");
        // Fail the running task and make sure it restarts properly
        assertThat(new TestTasksRequestBuilder(client()).setOperation("fail").setTaskId(firstRunningTask.getTaskId())
                .get().getTasks().size(), equalTo(1));

        logger.info("Waiting for persistent task with id {} to disappear", firstRunningTask.getId());
        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(),
                    empty());
        });
    }

    public void testPersistentActionCompletion() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.startPersistentTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        long allocationId = future.get().getAllocationId();
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks().size(), equalTo(1));
        });
        TaskInfo firstRunningTask = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .setDetailed(true).get().getTasks().get(0);
        logger.info("Found running task with id {} and parent {}", firstRunningTask.getId(), firstRunningTask.getParentTaskId());
        // Verifying parent and description
        assertThat(firstRunningTask.getParentTaskId().getId(), equalTo(allocationId));
        assertThat(firstRunningTask.getParentTaskId().getNodeId(), equalTo("cluster"));
        assertThat(firstRunningTask.getDescription(), equalTo("id=" + taskId));

        if (randomBoolean()) {
            logger.info("Simulating errant completion notification");
            //try sending completion request with incorrect allocation id
            PlainActionFuture<PersistentTask<?>> failedCompletionNotificationFuture = new PlainActionFuture<>();
            persistentTasksService.sendCompletionNotification(taskId, Long.MAX_VALUE, null, failedCompletionNotificationFuture);
            assertThrows(failedCompletionNotificationFuture, ResourceNotFoundException.class);
            // Make sure that the task is still running
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                    .setDetailed(true).get().getTasks().size(), equalTo(1));
        }

        stopOrCancelTask(firstRunningTask.getTaskId());
    }

    public void testPersistentActionWithNoAvailableNode() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        testParams.setExecutorNodeAttr("test");
        persistentTasksService.startPersistentTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, testParams, future);
        String taskId = future.get().getId();

        Settings nodeSettings = Settings.builder().put(nodeSettings(0)).put("node.attr.test_attr", "test").build();
        String newNode = internalCluster().startNode(nodeSettings);
        String newNodeId = internalCluster().clusterService(newNode).localNode().getId();
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks()
                    .size(), equalTo(1));
        });
        TaskInfo taskInfo = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);

        // Verifying the the task runs on the new node
        assertThat(taskInfo.getTaskId().getNodeId(), equalTo(newNodeId));

        internalCluster().stopRandomNode(settings -> "test".equals(settings.get("node.attr.test_attr")));

        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(),
                    empty());
        });

        // Remove the persistent task
        PlainActionFuture<PersistentTask<?>> removeFuture = new PlainActionFuture<>();
        persistentTasksService.cancelPersistentTask(taskId, removeFuture);
        assertEquals(removeFuture.get().getId(), taskId);
    }

    public void testPersistentActionStatusUpdate() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.startPersistentTask(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        String taskId = future.get().getId();

        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks()
                    .size(), equalTo(1));
        });
        TaskInfo firstRunningTask = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);

        PersistentTasksCustomMetaData tasksInProgress = internalCluster().clusterService().state().getMetaData()
                .custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(1));
        assertThat(tasksInProgress.tasks().iterator().next().getStatus(), nullValue());

        int numberOfUpdates = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfUpdates; i++) {
            logger.info("Updating the task status");
            // Complete the running task and make sure it finishes properly
            assertThat(new TestTasksRequestBuilder(client()).setOperation("update_status").setTaskId(firstRunningTask.getTaskId())
                    .get().getTasks().size(), equalTo(1));

            int finalI = i;
            WaitForPersistentTaskStatusFuture<?> future1 = new WaitForPersistentTaskStatusFuture<>();
            persistentTasksService.waitForPersistentTaskStatus(taskId,
                    task -> task != null && task.getStatus() != null && task.getStatus().toString() != null &&
                            task.getStatus().toString().equals("{\"phase\":\"phase " + (finalI + 1) + "\"}"),
                    TimeValue.timeValueSeconds(10), future1);
            assertThat(future1.get().getId(), equalTo(taskId));
        }

        WaitForPersistentTaskStatusFuture<?> future1 = new WaitForPersistentTaskStatusFuture<>();
        persistentTasksService.waitForPersistentTaskStatus(taskId,
                task -> false, TimeValue.timeValueMillis(10), future1);

        assertThrows(future1, IllegalStateException.class, "timed out after 10ms");

        PlainActionFuture<PersistentTask<?>> failedUpdateFuture = new PlainActionFuture<>();
        persistentTasksService.updateStatus(taskId, -2, new Status("should fail"), failedUpdateFuture);
        assertThrows(failedUpdateFuture, ResourceNotFoundException.class, "the task with id " + taskId +
                " and allocation id -2 doesn't exist");

        // Wait for the task to disappear
        WaitForPersistentTaskStatusFuture<?> future2 = new WaitForPersistentTaskStatusFuture<>();
        persistentTasksService.waitForPersistentTaskStatus(taskId, Objects::isNull, TimeValue.timeValueSeconds(10), future2);

        logger.info("Completing the running task");
        // Complete the running task and make sure it finishes properly
        assertThat(new TestTasksRequestBuilder(client()).setOperation("finish").setTaskId(firstRunningTask.getTaskId())
                .get().getTasks().size(), equalTo(1));

        assertThat(future2.get(), nullValue());
    }

    public void testCreatePersistentTaskWithDuplicateId() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.startPersistentTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        future.get();

        PlainActionFuture<PersistentTask<TestParams>> future2 = new PlainActionFuture<>();
        persistentTasksService.startPersistentTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future2);
        assertThrows(future2, ResourceAlreadyExistsException.class);

        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks().size(), equalTo(1));
        });

        TaskInfo firstRunningTask = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);

        logger.info("Completing the running task");
        // Fail the running task and make sure it restarts properly
        assertThat(new TestTasksRequestBuilder(client()).setOperation("finish").setTaskId(firstRunningTask.getTaskId())
                .get().getTasks().size(), equalTo(1));

        logger.info("Waiting for persistent task with id {} to disappear", firstRunningTask.getId());
        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(),
                    empty());
        });
    }

    private void stopOrCancelTask(TaskId taskId) {
        if (randomBoolean()) {
            logger.info("Completing the running task");
            // Complete the running task and make sure it finishes properly
            assertThat(new TestTasksRequestBuilder(client()).setOperation("finish").setTaskId(taskId)
                    .get().getTasks().size(), equalTo(1));

        } else {
            logger.info("Cancelling the running task");
            // Cancel the running task and make sure it finishes properly
            assertThat(client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
                    .get().getTasks().size(), equalTo(1));
        }


    }

    private void assertNoRunningTasks() throws Exception {
        assertBusy(() -> {
            // Wait for the task to finish
            List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks();
            logger.info("Found {} tasks", tasks.size());
            assertThat(tasks.size(), equalTo(0));

            // Make sure the task is removed from the cluster state
            assertThat(((PersistentTasksCustomMetaData) internalCluster().clusterService().state().getMetaData()
                    .custom(PersistentTasksCustomMetaData.TYPE)).tasks(), empty());
        });
    }

}