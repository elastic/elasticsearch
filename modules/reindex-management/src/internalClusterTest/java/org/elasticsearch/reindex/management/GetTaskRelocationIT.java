/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@code GET _tasks/{task_id}} transparently following reindex relocation chains.
 * <p>
 * Each test starts a cluster with a dedicated data-holding node (the "survivor" that is never shut down)
 * and one or more ephemeral nodes. A throttled async reindex is started on the first ephemeral node, then
 * successive shutdowns trigger relocations. {@code GET _tasks/{originalTaskId}} must resolve through
 * the full relocation chain.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class GetTaskRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    private final int requestsPerSecond = randomIntBetween(bulkSize * numOfSlices, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

    @BeforeClass
    public static void skipIfReindexResilienceDisabled() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class, MockTransportService.TestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testGetTaskFollowsRelocation() throws Exception {
        final ClusterSetup setup = startClusterAndReindex();

        shutdownAndRelocate(setup.reindexNodeName);
        final TaskId relocatedTaskId = readRelocatedTaskId(setup.originalTaskId);

        // we have millisecond precision here, so leave space for 1ms because runningTimeInNanos has nanosecond resolution
        final long minNanosElapsedSinceStart = TimeUnit.MILLISECONDS.toNanos(
            System.currentTimeMillis() - setup.originalStartTimeMillis - 1
        );
        final TaskResult runningResult = getTask(setup.originalTaskId, false).getTask();

        assertThat("should not be completed (relocated task is still running)", runningResult.isCompleted(), is(false));
        assertThat("should be a reindex action", runningResult.getTask().action(), equalTo(ReindexAction.NAME));
        assertThat("task ID should be the relocated task", runningResult.getTask().taskId(), equalTo(relocatedTaskId));
        assertThat("node should be the survivor node", runningResult.getTask().node(), equalTo(setup.survivorNodeId));
        assertThat("start time should be from original task", runningResult.getTask().startTime(), equalTo(setup.originalStartTimeMillis));
        assertThat(
            "original task ID should reference the original",
            runningResult.getTask().originalTaskId(),
            equalTo(setup.originalTaskId)
        );
        assertThat(
            "original start time should match original",
            runningResult.getTask().originalStartTimeMillis(),
            equalTo(setup.originalStartTimeMillis)
        );
        assertThat(
            "running time should cover time since original start",
            runningResult.getTask().runningTimeNanos(),
            greaterThanOrEqualTo(minNanosElapsedSinceStart)
        );
        assertThat("status should be present", runningResult.getTask().status(), is(notNullValue()));
        assertThat("no error on running task", runningResult.getError(), is(nullValue()));
        assertThat("no response yet on running task", runningResult.getResponse(), is(nullValue()));

        // Direct get of the relocated task returns its own start_time (not the original's)
        final TaskResult directResult = getTask(relocatedTaskId, false).getTask();
        assertThat("direct get: task ID is the relocated task", directResult.getTask().taskId(), equalTo(relocatedTaskId));
        assertThat(
            "direct get: start_time is the relocated task's own (later than original)",
            directResult.getTask().startTime(),
            greaterThan(setup.originalStartTimeMillis)
        );
        assertThat(
            "direct get: original_task_id references the original",
            directResult.getTask().originalTaskId(),
            equalTo(setup.originalTaskId)
        );
        assertThat(
            "direct get: original_start_time is from the original",
            directResult.getTask().originalStartTimeMillis(),
            equalTo(setup.originalStartTimeMillis)
        );

        unthrottleReindex(relocatedTaskId);

        final TaskResult completedResult = getTask(setup.originalTaskId, true).getTask();
        assertThat("task should be completed after wait", completedResult.isCompleted(), is(true));
        assertThat("task ID should be the relocated task after completion", completedResult.getTask().taskId(), equalTo(relocatedTaskId));
        assertThat("start time preserved after completion", completedResult.getTask().startTime(), equalTo(setup.originalStartTimeMillis));
        assertThat(
            "original task ID preserved after completion",
            completedResult.getTask().originalTaskId(),
            equalTo(setup.originalTaskId)
        );
        assertThat(
            "original start time preserved after completion",
            completedResult.getTask().originalStartTimeMillis(),
            equalTo(setup.originalStartTimeMillis)
        );
        assertThat("completed task should have no error", completedResult.getError(), is(nullValue()));

        assertCompletedReindexResponse(completedResult);
    }

    /**
     * Tests that the Get Task API falls back to the {@code .tasks} index when the original task's node
     * is unreachable, then follows the relocation chain and waits for the relocated task to complete.
     */
    public void testGetTaskFallsBackToTasksIndexOnNodeDisconnect() throws Exception {
        final ClusterSetup setup = startClusterAndReindex();

        // Trigger relocation: .tasks is written, relocated task running on survivor. Reindex node still alive.
        internalCluster().getInstance(ShutdownPrepareService.class, setup.reindexNodeName).prepareForShutdown();
        assertTrue(".tasks index should exist after relocation", indexExists(TaskResultsService.TASK_INDEX));
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);

        final TaskId relocatedTaskId = readRelocatedTaskId(setup.originalTaskId);

        // Block the GetTask transport action from survivor → reindex node and simulate a ConnectTransportException
        MockTransportService survivorTransport = MockTransportService.getInstance(setup.survivorNodeName);
        survivorTransport.addFailToSendNoConnectRule(
            internalCluster().getInstance(TransportService.class, setup.reindexNodeName),
            Set.of(TransportGetTaskAction.TYPE.name())
        );

        // Unthrottle so the relocated task can finish
        unthrottleReindex(relocatedTaskId);

        // Get should still follow relocation chain using result in .tasks
        final GetTaskResponse response = getTask(setup.survivorNodeName, setup.originalTaskId, true);
        final TaskResult result = response.getTask();
        assertThat("task should be completed", result.isCompleted(), is(true));
        assertThat("start time should be from original task", result.getTask().startTime(), equalTo(setup.originalStartTimeMillis));
        assertThat("original task ID should be preserved", result.getTask().originalTaskId(), equalTo(setup.originalTaskId));
        assertThat("no error on completed task", result.getError(), is(nullValue()));
        assertCompletedReindexResponse(result);
    }

    /**
     * Tests that {@code GET _tasks/{originalTaskId}?follow_relocations=false} returns the raw task result
     * with the {@code task_relocated_exception} error instead of transparently following the relocation chain.
     */
    public void testGetTaskWithFollowRelocationsFalse() throws Exception {
        final ClusterSetup setup = startClusterAndReindex();

        shutdownAndRelocate(setup.reindexNodeName);
        final TaskId relocatedTaskId = readRelocatedTaskId(setup.originalTaskId);

        // While the relocated task is still running, follow_relocations=false should return the original's raw result
        assertRawRelocatedTask(setup.originalTaskId, relocatedTaskId);

        // Let the relocated task complete, then verify the stored result is unchanged
        unthrottleReindex(relocatedTaskId);
        getTask(relocatedTaskId, true);
        assertRawRelocatedTask(setup.originalTaskId, relocatedTaskId);
    }

    private void assertRawRelocatedTask(TaskId originalTaskId, TaskId expectedRelocatedTaskId) throws IOException {
        final Request request = new Request("GET", "/_tasks/" + originalTaskId);
        request.addParameter("follow_relocations", "false");
        final Response response = getRestClient().performRequest(request);
        final Map<String, Object> body = ESRestTestCase.entityAsMap(response);

        assertThat("task is completed", body.get("completed"), is(true));
        @SuppressWarnings("unchecked")
        final Map<String, Object> task = (Map<String, Object>) body.get("task");
        assertThat("task node is the original", task.get("node"), equalTo(originalTaskId.getNodeId()));
        assertThat("task id is the original", task.get("id"), equalTo((int) originalTaskId.getId()));
        @SuppressWarnings("unchecked")
        final Map<String, Object> error = (Map<String, Object>) body.get("error");
        assertThat("error type is task_relocated_exception", error.get("type"), equalTo("task_relocated_exception"));
        assertThat("relocated_task_id is present", error.get("relocated_task_id"), equalTo(expectedRelocatedTaskId.toString()));
    }

    // --- Setup ---

    private record ClusterSetup(
        String survivorNodeName,
        String survivorNodeId,
        String reindexNodeName,
        TaskId originalTaskId,
        long originalStartTimeMillis
    ) {}

    /**
     * Starts a two-node cluster: a survivor node (holds data, never shut down) and an ephemeral node
     * that runs a throttled async reindex. The ephemeral node can be shut down to trigger relocation.
     */
    private ClusterSetup startClusterAndReindex() throws Exception {
        final Settings nodeRoles = NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE));
        final String survivorNodeName = internalCluster().startNode(nodeRoles);
        final String survivorNodeId = nodeIdByName(survivorNodeName);
        final String reindexNodeName = internalCluster().startNode(nodeRoles);
        ensureStableCluster(2);

        createIndexPinnedToNode(SOURCE_INDEX, survivorNodeName);
        createIndexPinnedToNode(DEST_INDEX, survivorNodeName);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindex(reindexNodeName);

        final GetTaskResponse initialResponse = getTask(originalTaskId, false);
        assertThat("initial reindex should not be completed", initialResponse.getTask().isCompleted(), is(false));
        assertThat("initial task should be a reindex", initialResponse.getTask().getTask().action(), equalTo(ReindexAction.NAME));
        final long originalStartTimeMillis = initialResponse.getTask().getTask().startTime();

        return new ClusterSetup(survivorNodeName, survivorNodeId, reindexNodeName, originalTaskId, originalStartTimeMillis);
    }

    private void assertCompletedReindexResponse(TaskResult completedResult) {
        assertThat("completed task should have a response", completedResult.getResponse(), is(notNullValue()));

        Map<String, Object> response = completedResult.getResponseAsMap();
        assertThat("total documents should match indexed count", response.get("total"), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(
            "created documents should match indexed count",
            response.get("created"),
            is(numberOfDocumentsThatTakes60SecondsToIngest)
        );
        assertThat("no failures", ((List<?>) response.get("failures")).size(), is(0));
    }

    private GetTaskResponse getTask(TaskId taskId, boolean waitForCompletion) {
        return getTask(null, taskId, waitForCompletion);
    }

    private GetTaskResponse getTask(String nodeName, TaskId taskId, boolean waitForCompletion) {
        return client(nodeName).admin()
            .cluster()
            .getTask(
                new GetTaskRequest().setTaskId(taskId).setWaitForCompletion(waitForCompletion).setTimeout(TimeValue.timeValueSeconds(30))
            )
            .actionGet();
    }

    private void shutdownAndRelocate(String nodeName) throws Exception {
        internalCluster().getInstance(ShutdownPrepareService.class, nodeName).prepareForShutdown();
        assertTrue(".tasks index should exist after relocation", indexExists(TaskResultsService.TASK_INDEX));
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        internalCluster().stopNode(nodeName);
    }

    private TaskId readRelocatedTaskId(TaskId taskId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());

        final GetResponse getResponse = client().prepareGet(TaskResultsService.TASK_INDEX, taskId.toString()).get();
        assertThat("task [" + taskId + "] should exist in .tasks index", getResponse.isExists(), is(true));

        final TaskResult result;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, getResponse.getSourceAsString())
        ) {
            result = TaskResult.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }
        assertThat("task [" + taskId + "] should be completed", result.isCompleted(), is(true));

        final Map<String, Object> errorMap = result.getErrorAsMap();
        assertThat("error type should be task_relocated_exception", errorMap.get("type"), equalTo("task_relocated_exception"));
        final String relocatedId = (String) errorMap.get("relocated_task_id");
        assertNotNull("relocated_task_id should be present", relocatedId);
        return new TaskId(relocatedId);
    }

    private TaskId startAsyncThrottledReindex(String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", Integer.toString(numOfSlices));
            request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
            request.setJsonEntity(Strings.format("""
                {
                  "source": {
                    "index": "%s",
                    "size": %d
                  },
                  "dest": {
                    "index": "%s"
                  }
                }
                """, SOURCE_INDEX, bulkSize, DEST_INDEX));

            Response response = restClient.performRequest(request);
            String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private void unthrottleReindex(TaskId taskId) {
        try {
            Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
            request.addParameter("requests_per_second", Integer.toString(-1));
            getRestClient().performRequest(request);
        } catch (Exception e) {
            throw new AssertionError("failed to rethrottle reindex", e);
        }
    }

    private void createIndexPinnedToNode(String index, String nodeName) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", randomIntBetween(1, 3))
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), index);
    }

    private String nodeIdByName(String nodeName) {
        return clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("node with name [" + nodeName + "] not found"));
    }
}
