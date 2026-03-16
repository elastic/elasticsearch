/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration tests for {@code GET _reindex/{task_id}} transparently following relocation chains.
 * <p>
 * Uses a two-node cluster where nodeA hosts data and nodeB runs a throttled reindex.
 * Shutting down nodeB triggers task relocation to nodeA. Verifies that
 * {@code GET _reindex/{originalTaskId}} resolves through the relocation chain and
 * returns the relocated task's live state while preserving the original task's
 * identity and start time.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexGetRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 5);
    private final int requestsPerSecond = randomIntBetween(1, 5);
    private final int numOfSlices = randomIntBetween(1, 10);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond * bulkSize;

    @BeforeClass
    public static void skipIfReindexResilienceDisabled() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testGetReindexFollowsRelocation() throws Exception {
        final ReindexSetup setup = setupTwoNodeReindex();

        // trigger relocation: shutdown nodeB
        shutdownNodeNameAndRelocate(setup.nodeBName);

        final TaskId relocatedTaskId = getRelocatedTaskIdFromTasksIndex(setup.originalTaskId, setup.nodeAId);

        // GET _reindex/{originalTaskId} after relocation
        final GetReindexResponse relocatedResponse = getReindexWithWaitForCompletion(setup.originalTaskId, false);
        final TaskResult completedOriginal = relocatedResponse.getOriginalTask();
        assertThat("original task should be completed", completedOriginal.isCompleted(), is(true));
        assertThat("original start time preserved", completedOriginal.getTask().startTime(), equalTo(setup.originalStartTimeMillis));

        final TaskResult relocatedTask = relocatedResponse.getRelocatedTask().orElse(null);
        assertNotNull("relocation should be present", relocatedTask);
        final TaskInfo relocatedInfo = relocatedTask.getTask();
        assertThat("relocated task ID matches .tasks index", relocatedInfo.taskId(), equalTo(relocatedTaskId));
        assertThat("relocated task should not be completed", relocatedTask.isCompleted(), is(false));
        assertThat("relocated task should be on nodeA", relocatedInfo.taskId().getNodeId(), equalTo(setup.nodeAId));
        assertThat("relocated task should not be cancelled", relocatedInfo.cancelled(), is(false));
        assertThat("relocated task status should be present", relocatedInfo.status(), is(notNullValue()));
        assertThat("relocated task started after original", relocatedInfo.startTime(), greaterThan(setup.originalStartTimeMillis));
        assertThat("relocated task has no error", relocatedTask.getError(), is(nullValue()));

        final long nanosElapsedFromReindexStart = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - setup.originalStartTimeMillis);

        // Speed up reindex post-relocation to keep the test fast
        unthrottleReindex(relocatedTaskId);

        final GetReindexResponse completedResponse = getReindexWithWaitForCompletion(setup.originalTaskId, true);
        assertCompletedReindexResponse(
            completedResponse,
            setup.originalTaskId,
            setup.originalStartTimeMillis,
            nanosElapsedFromReindexStart
        );
    }

    /**
     * Test that a {@code wait_for_completion=true} GET, issued to nodeA that isn't shutting down, while the reindex is still running
     * on nodeB, returns a correct completed response after the task is relocated to nodeA,
     * instead of returning TaskRelocatedException to user.
     */
    public void testGetWaitForCompletionDuringRelocation() throws Exception {
        final ReindexSetup setup = setupTwoNodeReindex();

        // Fire a blocking wait_for_completion=true GET in a background thread before relocation
        final PlainActionFuture<GetReindexResponse> waitFuture = new PlainActionFuture<>();
        final Thread waitThread = new Thread(() -> {
            try {
                waitFuture.onResponse(getReindexOnNodeWithWaitForCompletion(setup.nodeAName, setup.originalTaskId, true));
            } catch (Exception e) {
                waitFuture.onFailure(e);
            }
        }, "wait-for-completion-thread");
        waitThread.start();

        // trigger relocation: shutdown nodeB
        shutdownNodeNameAndRelocate(setup.nodeBName);

        // Speed up reindex post-relocation to keep the test fast
        final TaskId relocatedTaskId = getRelocatedTaskIdFromTasksIndex(setup.originalTaskId, setup.nodeAId);
        final long nanosElapsedFromReindexStart = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - setup.originalStartTimeMillis);
        unthrottleReindex(relocatedTaskId);

        // The background wait_for_completion=true GET should return a correct completed response
        final GetReindexResponse response = waitFuture.actionGet(60, TimeUnit.SECONDS);
        waitThread.join(10_000);
        assertCompletedReindexResponse(response, setup.originalTaskId, setup.originalStartTimeMillis, nanosElapsedFromReindexStart);
    }

    private record ReindexSetup(
        String nodeAName,
        String nodeAId,
        String nodeBName,
        String nodeBId,
        TaskId originalTaskId,
        long originalStartTimeMillis
    ) {
        private ReindexSetup {
            Stream.of(nodeAName, nodeAId, nodeBName, nodeBId, originalTaskId).forEach(Objects::requireNonNull);
        }
    }

    /**
     * Starts a two-node cluster, creates source/dest indices pinned to nodeA, indexes documents,
     * starts a throttled async reindex on nodeB, and verifies the initial task state.
     */
    private ReindexSetup setupTwoNodeReindex() throws Exception {
        final String nodeAName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String nodeAId = nodeIdByName(nodeAName);
        final String nodeBName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String nodeBId = nodeIdByName(nodeBName);
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, nodeAName);
        createIndexPinnedToNodeName(DEST_INDEX, nodeAName);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(nodeBName);

        final GetReindexResponse initialResponse = getReindexWithWaitForCompletion(originalTaskId, false);
        final TaskResult initialTask = initialResponse.getOriginalTask();
        final TaskInfo initialInfo = initialTask.getTask();
        assertThat("initial reindex should not be completed", initialTask.isCompleted(), is(false));
        assertThat("no relocation yet", initialResponse.getRelocatedTask().isPresent(), is(false));
        assertThat("task should be on nodeB", initialInfo.taskId().getNodeId(), equalTo(nodeBId));
        assertThat("task should not be cancelled", initialInfo.cancelled(), is(false));
        assertThat("status should be present", initialInfo.status(), is(notNullValue()));
        assertThat("no error", initialTask.getError(), is(nullValue()));

        return new ReindexSetup(nodeAName, nodeAId, nodeBName, nodeBId, originalTaskId, initialInfo.startTime());
    }

    /**
     * Asserts that a completed reindex response preserves the original task identity and start time,
     * contains no errors, reports correct document counts, and matches a subsequent non-waiting GET.
     */
    private void assertCompletedReindexResponse(
        GetReindexResponse completedResponse,
        TaskId originalTaskId,
        long originalStartTimeMillis,
        long nanosElapsedFromReindexStart
    ) throws IOException {
        final Map<String, Object> responseMap = XContentTestUtils.convertToMap(completedResponse);

        final GetReindexResponse nonWaitingResponse = getReindexWithWaitForCompletion(originalTaskId, false);
        final Map<String, Object> nonWaitingMap = XContentTestUtils.convertToMap(nonWaitingResponse);

        assertThat("wait and non-wait responses are identical", nonWaitingResponse, equalTo(completedResponse));
        assertThat("wait and non-wait responses should serialize identically", nonWaitingMap, equalTo(responseMap));
        assertThat("response completed", responseMap.get("completed"), is(true));
        assertThat("response not cancelled", responseMap.get("cancelled"), is(false));
        assertThat("response exists", responseMap.get("response"), is(notNullValue()));
        assertThat("no error nor relocations in final response", responseMap.get("error"), is(nullValue()));
        assertThat("original task ID node preserved", responseMap.get("id"), equalTo(originalTaskId.toString()));
        assertThat("original start time still preserved", responseMap.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        final long runningTimeNanos = ((Number) responseMap.get("running_time_in_nanos")).longValue();
        assertThat("running time nanos takes relocations into account", runningTimeNanos, greaterThan(nanosElapsedFromReindexStart));

        assertThat("total documents", ObjectPath.eval("response.total", responseMap), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat("created documents", ObjectPath.eval("response.created", responseMap), is(numberOfDocumentsThatTakes60SecondsToIngest));

        assertDocCount(DEST_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
    }

    private void shutdownNodeNameAndRelocate(final String nodeName) throws Exception {
        // testing assumption: .tasks should not exist yet — it's created when the task result is stored during relocation
        assertFalse(".tasks index should not exist before shutdown", indexExists(TaskResultsService.TASK_INDEX));

        // trigger reindex relocation
        internalCluster().getInstance(ShutdownPrepareService.class, nodeName).prepareForShutdown();

        // Wait for .tasks and replica to be created before stopping nodeB, otherwise the replica
        // on nodeA is stale and can't be promoted to primary when nodeB leaves
        assertBusy(() -> assertTrue(indexExists(TaskResultsService.TASK_INDEX)), 30, TimeUnit.SECONDS);
        ensureGreen(TaskResultsService.TASK_INDEX);

        internalCluster().stopNode(nodeName);
    }

    private GetReindexResponse getReindexOnNodeWithWaitForCompletion(String nodeName, TaskId taskId, boolean waitForCompletion) {
        return internalCluster().client(nodeName)
            .execute(TransportGetReindexAction.TYPE, new GetReindexRequest(taskId, waitForCompletion, TimeValue.timeValueSeconds(30)))
            .actionGet();
    }

    private GetReindexResponse getReindexWithWaitForCompletion(TaskId taskId, boolean waitForCompletion) {
        return client().execute(
            TransportGetReindexAction.TYPE,
            new GetReindexRequest(taskId, waitForCompletion, TimeValue.timeValueSeconds(30))
        ).actionGet();
    }

    private TaskId getRelocatedTaskIdFromTasksIndex(TaskId originalTaskId, String expectedNodeId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());

        final GetResponse getResponse = client().prepareGet(TaskResultsService.TASK_INDEX, originalTaskId.toString()).get();
        assertThat("task exists in .tasks index", getResponse.isExists(), is(true));

        final TaskResult result;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, getResponse.getSourceAsString())
        ) {
            result = TaskResult.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }
        assertThat("original task should be completed", result.isCompleted(), is(true));

        final Map<String, Object> errorMap = result.getErrorAsMap();
        assertThat(errorMap.get("type"), equalTo("task_relocated_exception"));
        final String relocatedId = (String) errorMap.get("relocated_task_id");
        assertThat("relocated to expected node", relocatedId, startsWith(expectedNodeId));
        return new TaskId(relocatedId);
    }

    private TaskId startAsyncThrottledReindexOnNode(String nodeName) throws Exception {
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

    private void createIndexPinnedToNodeName(String index, String nodeName) {
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

    private void assertDocCount(String index, int expected) throws IOException {
        assertNoFailures(indicesAdmin().prepareRefresh(index).get());
        Request request = new Request("GET", "/" + index + "/_count");
        Response response = getRestClient().performRequest(request);
        int count = ((Number) ESRestTestCase.entityAsMap(response).get("count")).intValue();
        assertThat(count, equalTo(expected));
    }
}
