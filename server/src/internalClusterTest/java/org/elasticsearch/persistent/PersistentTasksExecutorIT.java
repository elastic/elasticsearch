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

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class PersistentTasksExecutorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Before
    public void resetNonClusterStateCondition() {
        TestPersistentTasksExecutor.setNonClusterStateCondition(true);
    }

    @After
    public void cleanup() throws Exception {
        assertNoRunningTasks();
    }

    public static class WaitForPersistentTaskFuture<Params extends PersistentTaskParams>
            extends PlainActionFuture<PersistentTask<Params>>
            implements WaitForPersistentTaskListener<Params> {
    }

    public void testPersistentActionFailure() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
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
        persistentTasksService.sendStartRequest(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
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
            persistentTasksService.sendCompletionRequest(taskId, Long.MAX_VALUE, null, failedCompletionNotificationFuture);
            assertFutureThrows(failedCompletionNotificationFuture, ResourceNotFoundException.class);
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
        persistentTasksService.sendStartRequest(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, testParams, future);
        String taskId = future.get().getId();

        Settings nodeSettings = Settings.builder().put(nodeSettings(0)).put("node.attr.test_attr", "test").build();
        String newNode = internalCluster().startNode(nodeSettings);
        String newNodeId = internalCluster().clusterService(newNode).localNode().getId();
        waitForTaskToStart();

        TaskInfo taskInfo = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);

        // Verifying the task runs on the new node
        assertThat(taskInfo.getTaskId().getNodeId(), equalTo(newNodeId));

        internalCluster().stopRandomNode(settings -> "test".equals(settings.get("node.attr.test_attr")));

        assertBusy(() -> {
            // Wait for the task to disappear completely
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(),
                    empty());
        });

        // Remove the persistent task
        PlainActionFuture<PersistentTask<?>> removeFuture = new PlainActionFuture<>();
        persistentTasksService.sendRemoveRequest(taskId, removeFuture);
        assertEquals(removeFuture.get().getId(), taskId);
    }

    public void testPersistentActionWithNonClusterStateCondition() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService =
            internalCluster().getInstance(PersistentTasksClusterService.class, internalCluster().getMasterName());
        // Speed up rechecks to a rate that is quicker than what settings would allow
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(1));

        TestPersistentTasksExecutor.setNonClusterStateCondition(false);

        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        persistentTasksService.sendStartRequest(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, testParams, future);
        String taskId = future.get().getId();

        assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks(),
            empty());

        TestPersistentTasksExecutor.setNonClusterStateCondition(true);

        waitForTaskToStart();
        TaskInfo taskInfo = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get().getTasks().get(0);

        // Verifying the task can now be assigned
        assertThat(taskInfo.getTaskId().getNodeId(), notNullValue());

        // Remove the persistent task
        PlainActionFuture<PersistentTask<?>> removeFuture = new PlainActionFuture<>();
        persistentTasksService.sendRemoveRequest(taskId, removeFuture);
        assertEquals(removeFuture.get().getId(), taskId);
    }

    public void testPersistentActionStatusUpdate() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        String taskId = future.get().getId();
        waitForTaskToStart();
        TaskInfo firstRunningTask = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get().getTasks().get(0);

        PersistentTasksCustomMetadata tasksInProgress = internalCluster().clusterService().state().getMetadata()
                .custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(1));
        assertThat(tasksInProgress.tasks().iterator().next().getState(), nullValue());

        int numberOfUpdates = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfUpdates; i++) {
            logger.info("Updating the task states");
            // Complete the running task and make sure it finishes properly
            assertThat(new TestTasksRequestBuilder(client()).setOperation("update_status").setTaskId(firstRunningTask.getTaskId())
                    .get().getTasks().size(), equalTo(1));

            int finalI = i;
            WaitForPersistentTaskFuture<?> future1 = new WaitForPersistentTaskFuture<>();
            persistentTasksService.waitForPersistentTaskCondition(taskId,
                    task -> task != null && task.getState() != null && task.getState().toString() != null &&
                            task.getState().toString().equals("{\"phase\":\"phase " + (finalI + 1) + "\"}"),
                    TimeValue.timeValueSeconds(10), future1);
            assertThat(future1.get().getId(), equalTo(taskId));
        }

        WaitForPersistentTaskFuture<?> future1 = new WaitForPersistentTaskFuture<>();
        persistentTasksService.waitForPersistentTaskCondition(taskId,
                task -> false, TimeValue.timeValueMillis(10), future1);

        assertFutureThrows(future1, IllegalStateException.class, "timed out after 10ms");

        PlainActionFuture<PersistentTask<?>> failedUpdateFuture = new PlainActionFuture<>();
        persistentTasksService.sendUpdateStateRequest(taskId, -2, new State("should fail"), failedUpdateFuture);
        assertFutureThrows(failedUpdateFuture, ResourceNotFoundException.class, "the task with id " + taskId +
                " and allocation id -2 doesn't exist");

        // Wait for the task to disappear
        WaitForPersistentTaskFuture<?> future2 = new WaitForPersistentTaskFuture<>();
        persistentTasksService.waitForPersistentTaskCondition(taskId, Objects::isNull, TimeValue.timeValueSeconds(10), future2);

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
        persistentTasksService.sendStartRequest(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        future.get();

        PlainActionFuture<PersistentTask<TestParams>> future2 = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(taskId, TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future2);
        assertFutureThrows(future2, ResourceAlreadyExistsException.class);

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

    public void testUnassignRunningPersistentTask() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService =
            internalCluster().getInstance(PersistentTasksClusterService.class, internalCluster().getMasterName());
        // Speed up rechecks to a rate that is quicker than what settings would allow
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(1));
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
        TestParams testParams = new TestParams("Blah");
        testParams.setExecutorNodeAttr("test");
        persistentTasksService.sendStartRequest(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME, testParams, future);
        PersistentTask<TestParams> task = future.get();
        String taskId = task.getId();

        Settings nodeSettings = Settings.builder().put(nodeSettings(0)).put("node.attr.test_attr", "test").build();
        internalCluster().startNode(nodeSettings);

        waitForTaskToStart();

        PlainActionFuture<PersistentTask<?>> unassignmentFuture = new PlainActionFuture<>();

        // Disallow re-assignment after it is unallocated to verify master and node state
        TestPersistentTasksExecutor.setNonClusterStateCondition(false);

        persistentTasksClusterService.unassignPersistentTask(taskId,
            task.getAllocationId() + 1,
            "unassignment test",
            unassignmentFuture);
        PersistentTask<?> unassignedTask = unassignmentFuture.get();
        assertThat(unassignedTask.getId(), equalTo(taskId));
        assertThat(unassignedTask.getAssignment().getExplanation(), equalTo("unassignment test"));
        assertThat(unassignedTask.getAssignment().getExecutorNode(), is(nullValue()));

        assertBusy(() -> {
            // Verify that the task is NOT running on the node
            List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                .getTasks();
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
        TaskInfo taskInfo = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]")
            .get().getTasks().get(0);
        stopOrCancelTask(taskInfo.getTaskId());
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

    private static void waitForTaskToStart() throws Exception {
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks()
                .size(), equalTo(1));
        });
    }

    private static void assertClusterStateHasTask(String taskId) {
        Collection<PersistentTask<?>> clusterTasks = ((PersistentTasksCustomMetadata) internalCluster()
            .clusterService()
            .state()
            .getMetadata()
            .custom(PersistentTasksCustomMetadata.TYPE))
            .tasks();
        assertThat(clusterTasks, hasSize(1));
        assertThat(clusterTasks.iterator().next().getId(), equalTo(taskId));
    }

    private void assertNoRunningTasks() throws Exception {
        assertBusy(() -> {
            // Wait for the task to finish
            List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks();
            logger.info("Found {} tasks", tasks.size());
            assertThat(tasks.size(), equalTo(0));

            // Make sure the task is removed from the cluster state
            assertThat(((PersistentTasksCustomMetadata) internalCluster().clusterService().state().getMetadata()
                    .custom(PersistentTasksCustomMetadata.TYPE)).tasks(), empty());
        });
    }

}
