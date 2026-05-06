/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
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
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@code POST _reindex/{task_id}/_rethrottle} transparently following relocation chains.
 * <p>
 * Uses a two-node cluster where nodeA hosts data and nodeB runs a throttled reindex.
 * Shutting down nodeB triggers task relocation to nodeA. Verifies that
 * {@code POST _reindex/{originalTaskId}/_rethrottle} resolves through the relocation
 * chain and applies the new throttle to the relocated task.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexRethrottleRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    // keep RPS reasonable so each slice doesn't sleep and delay relocation for too long (max 1s)
    private final int requestsPerSecond = randomIntBetween(bulkSize * numOfSlices, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

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

    public void testRethrottleByOriginalTaskIdAfterRelocation() throws Exception {
        final ReindexSetup setup = setupTwoNodeReindex();

        // trigger relocation: shutdown nodeB
        shutdownNodeNameAndRelocate(setup.nodeBName);

        // 1st rethrottle: change rate to 2x — exercises chain-following
        final int newRps = requestsPerSecond * 2;
        final long timeMillisBeforeFirstRethrottle = System.currentTimeMillis();
        final Map<String, Object> rethrottleBody = rethrottleReindex(setup.originalTaskId, newRps);
        assertRethrottleHasTasksAndNoFailures(
            rethrottleBody,
            setup.originalTaskId,
            setup.originalStartTimeMillis,
            timeMillisBeforeFirstRethrottle
        );

        // GET by original task ID: verify the response shows the relocated task on nodeA
        final GetReindexResponse afterFirstRethrottle = getReindexWithWaitForCompletion(setup.originalTaskId, false);
        final TaskResult afterRethrottleResult = afterFirstRethrottle.getTaskResult();
        final TaskInfo afterRethrottleInfo = afterRethrottleResult.getTask();
        assertThat("task should still be running", afterRethrottleResult.isCompleted(), is(false));
        assertThat("task relocated to nodeA", afterRethrottleInfo.taskId().getNodeId(), equalTo(setup.nodeAId));
        assertThat("original task ID preserved", afterRethrottleInfo.originalTaskId(), equalTo(setup.originalTaskId));
        assertThat("start time adjusted to original", afterRethrottleInfo.startTime(), equalTo(setup.originalStartTimeMillis));
        // verify the rethrottle took effect on the leader's source-of-truth RPS (via XContent)
        final Map<String, Object> afterRethrottleMap = XContentTestUtils.convertToMap(afterFirstRethrottle);
        assertThat(ObjectPath.eval("status.requests_per_second", afterRethrottleMap), closeTo(newRps, 0.00001d));
        // verify the rethrottle took effect on the running task's slices (or worker)
        assertRethrottledRps(afterRethrottleInfo.taskId(), newRps);

        // 2nd rethrottle: unlimited — exercises chain-following a second time
        final long timeMillisBeforeSecondRethrottle = System.currentTimeMillis();
        final Map<String, Object> unthrottleBody = rethrottleReindex(setup.originalTaskId, -1);
        assertRethrottleHasTasksAndNoFailures(
            unthrottleBody,
            setup.originalTaskId,
            setup.originalStartTimeMillis,
            timeMillisBeforeSecondRethrottle
        );

        // 15s timeout is well under the 60s the throttled reindex would take,
        // so completing within this window proves the unlimited rate was applied
        final GetReindexResponse completedResponse = getReindexWithWaitForCompletion(setup.originalTaskId, true);
        final Map<String, Object> responseMap = XContentTestUtils.convertToMap(completedResponse);
        assertThat(responseMap.get("completed"), is(true));
        assertThat(responseMap.get("error"), is(nullValue()));
        assertThat(responseMap.get("id"), equalTo(setup.originalTaskId.toString()));
        assertThat(responseMap.get("start_time_in_millis"), equalTo(setup.originalStartTimeMillis));
        assertThat(responseMap.get("original_task_id"), is(nullValue()));
        assertThat(responseMap.get("original_start_time_in_millis"), is(nullValue()));
        assertThat(ObjectPath.eval("response.requests_per_second", responseMap), is(-1.0));
        assertThat(ObjectPath.eval("response.total", responseMap), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(ObjectPath.eval("response.created", responseMap), is(numberOfDocumentsThatTakes60SecondsToIngest));

        assertDocCount(DEST_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);

        // rethrottle after completion: nodeB is gone so we get a node failure, but no tasks
        final Map<String, Object> postCompletionBody = rethrottleReindex(setup.originalTaskId, randomIntBetween(1, 100));
        assertRethrottleNoTasksAndNodeFailure(postCompletionBody, setup.nodeBId);
    }

    private void assertRethrottleHasTasksAndNoFailures(
        final Map<String, Object> body,
        final TaskId originalTaskId,
        final long originalStartTimeMillis,
        final long timeMillisBeforeRethrottle
    ) {
        final Map<String, Object> tasks = ObjectPath.eval("nodes." + originalTaskId.getNodeId() + ".tasks", body);
        assertThat("rethrottle should have tasks on expected node", tasks, is(notNullValue()));
        assertThat("rethrottle should have exactly one task", tasks.size(), equalTo(1));
        final Map<String, Object> soleTask = ObjectPath.eval(originalTaskId.toString(), tasks);
        assertThat("task body id", ((Number) soleTask.get("id")).longValue(), equalTo(originalTaskId.getId()));
        assertThat("task body start_time_in_millis", soleTask.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        final long runningNanos = ((Number) soleTask.get("running_time_in_nanos")).longValue();
        // we have millisecond precision here, so leave space for 1ms because runningTimeInNanos has nanosecond resolution
        final long expectedMinimumRunningTimeNanos = TimeUnit.MILLISECONDS.toNanos(
            timeMillisBeforeRethrottle - originalStartTimeMillis - 1
        );
        assertThat(runningNanos, greaterThanOrEqualTo(expectedMinimumRunningTimeNanos));
        assertThat("no originalTaskId", soleTask.get("original_task_id"), is(nullValue()));
        assertThat("no originalStartTimeMillis", soleTask.get("original_start_time_in_millis"), is(nullValue()));
        final List<Object> taskFailures = ObjectPath.eval("task_failures", body);
        final List<Object> nodeFailures = ObjectPath.eval("node_failures", body);
        if (taskFailures != null) {
            assertThat("rethrottle should have no task failures", taskFailures, empty());
        }
        if (nodeFailures != null) {
            assertThat("rethrottle should have no node failures", nodeFailures, empty());
        }
    }

    private void assertRethrottleNoTasksAndNodeFailure(final Map<String, Object> body, String deadNodeId) {
        final Map<String, Object> nodes = ObjectPath.eval("nodes", body);
        assertThat("rethrottle response should have no nodes with tasks", nodes.size(), equalTo(0));
        final List<Object> nodeFailures = ObjectPath.eval("node_failures", body);
        assertThat("should have exactly one node failure", nodeFailures, is(notNullValue()));
        assertThat(nodeFailures.size(), equalTo(1));
        final String failedNodeId = ObjectPath.eval("node_id", nodeFailures.getFirst());
        assertThat("node failure should be for the dead node", failedNodeId, equalTo(deadNodeId));
    }

    /**
     * Verifies the rethrottle took effect by checking per-slice (or worker) RPS.
     * For non-sliced (numOfSlices == 1): checks the worker's status directly.
     * For sliced: lists child tasks and checks each slice has the expected share of total RPS.
     */
    private void assertRethrottledRps(final TaskId relocatedTaskId, final int expectedTotalRps) {
        if (numOfSlices == 1) {
            final BulkByScrollTask.Status status = (BulkByScrollTask.Status) clusterAdmin().prepareGetTask(relocatedTaskId)
                .get()
                .getTask()
                .getTask()
                .status();
            assertThat("non-sliced task should have the new RPS", (double) status.getRequestsPerSecond(), closeTo(expectedTotalRps, 0.001));
        } else {
            final ListTasksResponse listResponse = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
            final TaskGroup group = listResponse.getTaskGroups()
                .stream()
                .filter(g -> g.taskInfo().taskId().equals(relocatedTaskId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("relocated task group not found"));
            assertThat("all slices should be registered", group.childTasks().size(), equalTo(numOfSlices));
            final float expectedPerSlice = (float) expectedTotalRps / numOfSlices;
            for (TaskGroup child : group.childTasks()) {
                final BulkByScrollTask.Status sliceStatus = (BulkByScrollTask.Status) child.task().status();
                assertThat(
                    "slice " + sliceStatus + " should have the rethrottled RPS",
                    (double) sliceStatus.getRequestsPerSecond(),
                    closeTo(expectedPerSlice, 0.001)
                );
            }
        }
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
        final TaskResult initialResult = initialResponse.getTaskResult();
        final TaskInfo initialInfo = initialResult.getTask();
        assertThat("initial reindex should not be completed", initialResult.isCompleted(), is(false));
        assertThat("task should be on nodeB", initialInfo.taskId().getNodeId(), equalTo(nodeBId));
        assertThat("task should not be cancelled", initialInfo.cancelled(), is(false));
        assertThat("status should be present", initialInfo.status(), is(notNullValue()));
        assertThat("no error", initialResult.getError(), is(nullValue()));

        return new ReindexSetup(nodeAName, nodeAId, nodeBName, nodeBId, originalTaskId, initialInfo.startTime());
    }

    private void shutdownNodeNameAndRelocate(final String nodeName) throws Exception {
        assertFalse(".tasks index should not exist before shutdown", indexExists(TaskResultsService.TASK_INDEX));

        internalCluster().getInstance(ShutdownPrepareService.class, nodeName).prepareForShutdown();

        assertTrue(indexExists(TaskResultsService.TASK_INDEX));
        ensureGreen(TaskResultsService.TASK_INDEX);

        internalCluster().stopNode(nodeName);
    }

    private GetReindexResponse getReindexWithWaitForCompletion(final TaskId taskId, final boolean waitForCompletion) {
        return client().execute(
            TransportGetReindexAction.TYPE,
            new GetReindexRequest(taskId, waitForCompletion, TimeValue.timeValueSeconds(15))
        ).actionGet();
    }

    private Map<String, Object> rethrottleReindex(final TaskId taskId, final int requestsPerSecond) throws Exception {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
        request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
        final Response response = getRestClient().performRequest(request);
        return ESRestTestCase.entityAsMap(response);
    }

    private TaskId startAsyncThrottledReindexOnNode(final String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
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

    private void createIndexPinnedToNodeName(final String index, final String nodeName) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", randomIntBetween(1, 3))
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), index);
    }

    private String nodeIdByName(final String nodeName) {
        return clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("node with name [" + nodeName + "] not found"));
    }

    private void assertDocCount(final String index, final int expected) throws IOException {
        assertNoFailures(indicesAdmin().prepareRefresh(index).get());
        final Request request = new Request("GET", "/" + index + "/_count");
        final Response response = getRestClient().performRequest(request);
        final int count = ((Number) ESRestTestCase.entityAsMap(response).get("count")).intValue();
        assertThat(count, equalTo(expected));
    }
}
