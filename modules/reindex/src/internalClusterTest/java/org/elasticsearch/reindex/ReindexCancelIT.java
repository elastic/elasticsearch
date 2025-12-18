/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.junit.Before;

import java.util.Optional;

import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/** Integration tests for <code>POST _reindex/{taskId}/_cancel</code> endpoint. */
public class ReindexCancelIT extends ReindexTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";
    private static final int BULK_SIZE = 1;
    private static final int REQUESTS_PER_SECOND = 1;
    private static final int NUM_OF_SLICES = 2;
    private static final int NUMBER_OF_DOCUMENTS_THAT_TAKES_30_SECS_TO_INGEST = 30 * REQUESTS_PER_SECOND * BULK_SIZE;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(ordinal, otherSettings)).build();
    }

    @Before
    public void setup() {
        createIndex(SOURCE_INDEX, DEST_INDEX);
        indexRandom(true, SOURCE_INDEX, NUMBER_OF_DOCUMENTS_THAT_TAKES_30_SECS_TO_INGEST);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);
    }

    /**
     * Test <code>POST _reindex/{taskId}/_cancel</code> endpoint, and its intended side effects, end-to-end, by doing the following:
     * 1. Create throttled reindex task that takes a while to complete
     * 2. Ensure task has expected number of sub-tasks
     * 3. Ensure there's an expected number of search scroll contexts open for the reindexing
     * 4. Cancel reindex
     * 5. Ensure there's no failures, and all scroll contexts and sub-tasks are closed/cancelled
     * 6. Ensure reindex task and sub-tasks have correct cancelled reason
     * 7. Subsequent calls to cancel already-cancelled reindex task fail
     * <p>
     * We test synchronous (<code>?wait_for_completion=true</code>) invocation of the _cancel endpoint in this test.
     */
    public void testCancelEndpointEndToEndSynchronously() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskId parentTaskId = startAsyncThrottledReindex();

        final TaskInfo running = getRunningTask(parentTaskId);
        assertThat(running.description(), is("reindex from [" + SOURCE_INDEX + "] to [" + DEST_INDEX + "]"));
        assertThat(running.cancellable(), is(true));
        assertThat(running.cancelled(), is(false));

        final TaskGroup parent = findTaskGroup(parentTaskId).orElse(null);
        assertNotNull("parent group should exist", parent);
        assertThat(parent.childTasks().size(), equalTo(2));

        final TaskId firstSubTask = parent.childTasks().getFirst().task().taskId();
        final var cancelSubTaskException = expectThrows(ResourceNotFoundException.class, () -> cancelReindexSynchronously(firstSubTask));
        assertThat(
            cancelSubTaskException.getMessage(),
            is(Strings.format("reindex task [%s] either not found or completed", firstSubTask))
        );

        final int sourceIndexNumOfPrimaryShards = primaryShards(SOURCE_INDEX);
        assertBusy(() -> {
            final long currentScrollContexts = currentNumberOfScrollContexts();
            final long expectedScrollContexts = (long) sourceIndexNumOfPrimaryShards * NUM_OF_SLICES;
            assertThat("expected number of scroll contexts are open", currentScrollContexts, equalTo(expectedScrollContexts));
        });

        final CancelReindexResponse cancelResponse = cancelReindexSynchronously(parentTaskId);
        assertThat(cancelResponse.getTaskFailures(), empty());
        assertThat(cancelResponse.getNodeFailures(), empty());

        final var notFoundException = expectThrows(ResourceNotFoundException.class, () -> cancelReindexSynchronously(parentTaskId));
        assertThat(notFoundException.getMessage(), is(Strings.format("reindex task [%s] either not found or completed", parentTaskId)));
        assertThat("parent group should be absent", findTaskGroup(parentTaskId).isEmpty(), is(true));

        assertThat("there are no open scroll contexts", currentNumberOfScrollContexts(), equalTo(0L));

        final RawTaskStatus parentTaskStatus = (RawTaskStatus) getCompletedTaskResult(parentTaskId).getTask().status();
        final String cancelledReason = (String) parentTaskStatus.toMap().get("canceled");
        assertThat(cancelledReason, equalTo("by user request"));
    }

    /** Same test as above but calling _cancel asynchronously and wrapping assertions after cancellation in assertBusy. */
    public void testCancelEndpointEndToEndAsynchronously() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskId parentTaskId = startAsyncThrottledReindex();

        final TaskInfo running = getRunningTask(parentTaskId);
        assertThat(running.description(), is("reindex from [" + SOURCE_INDEX + "] to [" + DEST_INDEX + "]"));
        assertThat(running.cancellable(), is(true));
        assertThat(running.cancelled(), is(false));

        final TaskGroup parent = findTaskGroup(parentTaskId).orElse(null);
        assertNotNull("parent group should exist", parent);
        assertThat(parent.childTasks().size(), equalTo(2));

        final TaskId firstSubTask = parent.childTasks().getFirst().task().taskId();
        final var cancellingSubTaskException = expectThrows(
            ResourceNotFoundException.class,
            () -> cancelReindexSynchronously(firstSubTask)
        );
        assertThat(
            cancellingSubTaskException.getMessage(),
            is(Strings.format("reindex task [%s] either not found or completed", firstSubTask))
        );

        final int sourceIndexNumOfPrimaryShards = primaryShards(SOURCE_INDEX);
        assertBusy(() -> {
            final long currentScrollContexts = currentNumberOfScrollContexts();
            final long expectedScrollContexts = (long) sourceIndexNumOfPrimaryShards * NUM_OF_SLICES;
            assertThat("expected number of scroll contexts are open", currentScrollContexts, equalTo(expectedScrollContexts));
        });

        final CancelReindexResponse cancelResponse = cancelReindexAsynchronously(parentTaskId);
        assertThat(cancelResponse.getTaskFailures(), empty());
        assertThat(cancelResponse.getNodeFailures(), empty());

        assertBusy(() -> assertThat("there are no open scroll contexts", currentNumberOfScrollContexts(), equalTo(0L)));
        assertBusy(() -> assertThat("parent group should be absent", findTaskGroup(parentTaskId).isEmpty(), is(true)));
        assertBusy(() -> {
            final RawTaskStatus parentTaskStatus = (RawTaskStatus) getCompletedTaskResult(parentTaskId).getTask().status();
            final String cancelledReason = (String) parentTaskStatus.toMap().get("canceled");
            assertThat(cancelledReason, equalTo("by user request"));
        });

        final var notFoundException = expectThrows(ResourceNotFoundException.class, () -> cancelReindexAsynchronously(parentTaskId));
        assertThat(notFoundException.getMessage(), is(Strings.format("reindex task [%s] either not found or completed", parentTaskId)));
    }

    public void testCancellingNonexistingTaskOnExistingNode() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskId nonExistingTaskOnExistingNode = new TaskId(clusterService().localNode().getId(), Long.MAX_VALUE);

        final String expectedExceptionMessage = Strings.format(
            "reindex task [%s] either not found or completed",
            nonExistingTaskOnExistingNode
        );
        final var synchronousException = expectThrows(
            ResourceNotFoundException.class,
            () -> cancelReindexSynchronously(nonExistingTaskOnExistingNode)
        );
        assertThat(synchronousException.getMessage(), is(expectedExceptionMessage));

        final var asynchronousException = expectThrows(
            ResourceNotFoundException.class,
            () -> cancelReindexAsynchronously(nonExistingTaskOnExistingNode)
        );
        assertThat(asynchronousException.getMessage(), is(expectedExceptionMessage));
    }

    public void testCancellingTaskOnNonexistingNode() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskId taskId = new TaskId("non-existing-node-" + randomAlphaOfLength(8), randomLongBetween(1, 1_000_000L));

        final String expectedExceptionMessage = Strings.format("reindex task [%s] either not found or completed", taskId);

        final var synchronousException = expectThrows(ResourceNotFoundException.class, () -> cancelReindexSynchronously(taskId));
        assertThat(synchronousException.getMessage(), is(expectedExceptionMessage));

        final var asynchronousException = expectThrows(ResourceNotFoundException.class, () -> cancelReindexAsynchronously(taskId));
        assertThat(asynchronousException.getMessage(), is(expectedExceptionMessage));
    }

    private TaskId startAsyncThrottledReindex() throws Exception {
        final RestClient restClient = getRestClient();
        final Request request = new Request("POST", "/_reindex");
        request.addParameter("wait_for_completion", "false");
        request.addParameter("slices", Integer.toString(NUM_OF_SLICES));
        request.addParameter("requests_per_second", Integer.toString(REQUESTS_PER_SECOND));
        request.setJsonEntity(Strings.format("""
            {
              "source": {
                "index": "%s",
                "size": %d
              },
              "dest": {
                "index": "%s"
              }
            }""", SOURCE_INDEX, BULK_SIZE, DEST_INDEX));

        final Response response = restClient.performRequest(request);
        final String task = (String) entityAsMap(response).get("task");
        assertNotNull("reindex did not return a task id", task);
        return new TaskId(task);
    }

    private TaskInfo getRunningTask(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(false));
        return task.getTask();
    }

    private CancelReindexResponse cancelReindexSynchronously(final TaskId taskId) {
        final CancelReindexRequest request = new CancelReindexRequest(true);
        request.setTargetTaskId(taskId);
        request.setTimeout(TimeValue.timeValueSeconds(30));
        return client().execute(TransportCancelReindexAction.TYPE, request).actionGet();
    }

    private CancelReindexResponse cancelReindexAsynchronously(final TaskId taskId) {
        final CancelReindexRequest request = new CancelReindexRequest(false);
        request.setTargetTaskId(taskId);
        request.setTimeout(TimeValue.timeValueSeconds(30));
        return client().execute(TransportCancelReindexAction.TYPE, request).actionGet();
    }

    private Optional<TaskGroup> findTaskGroup(final TaskId taskId) {
        final ListTasksResponse response = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        return response.getTaskGroups().stream().filter(group -> group.taskInfo().taskId().equals(taskId)).findFirst();
    }

    private TaskResult getCompletedTaskResult(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(true));
        return task;
    }

    private long currentNumberOfScrollContexts() {
        final NodesStatsResponse stats = clusterAdmin().prepareNodesStats().clear().setIndices(true).get();
        long total = 0;
        for (var nodeStats : stats.getNodes()) {
            total += nodeStats.getIndices().getSearch().getTotal().getScrollCurrent();
        }
        return total;
    }

    private int primaryShards(final String index) {
        final TimeValue timeout = TimeValue.THIRTY_SECONDS;
        final var response = client().admin().indices().prepareGetIndex(timeout).addIndices(index).get();
        return Integer.parseInt(response.getSetting(index, IndexMetadata.SETTING_NUMBER_OF_SHARDS));
    }
}
