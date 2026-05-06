/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

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
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/// Integration test that relocates a reindex task twice across three nodes (A -> B -> C), then
/// exercises all reindex-management endpoints (rethrottle, list, get, cancel) against the
/// doubly-relocated task using the original task ID.
/// Node A (`firstCoordinatorNode`)     — starts reindex     → shutdown → relocates to B
/// Node B (`secondCoordinatorNode`)    — first relocation   → shutdown → relocates to C
/// Node C (`finalMasterDataNodeName`)  — holds indices, final relocation destination
/// {@code StatefulReindexRelocationNodePicker} prefers coordinating-only nodes, making the
/// relocation path deterministic: A → B → C.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexDoubleRelocationIT extends ESIntegTestCase {

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

    public void testDoubleRelocationWithManagementEndpoints() throws Exception {
        final String finalMasterDataNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String finalMasterDataNodeId = nodeIdByName(finalMasterDataNodeName);

        final String secondCoordinatorNodeName = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String secondCoordinatorNodeId = nodeIdByName(secondCoordinatorNodeName);

        final String firstCoordinatorNodeName = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String firstCoordinatorNodeId = nodeIdByName(firstCoordinatorNodeName);

        ensureStableCluster(3);

        createIndexPinnedToNodeName(SOURCE_INDEX, finalMasterDataNodeName);
        createIndexPinnedToNodeName(DEST_INDEX, finalMasterDataNodeName);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(firstCoordinatorNodeName);
        assertThat(originalTaskId.getNodeId(), equalTo(firstCoordinatorNodeId));

        final GetReindexResponse initialResponse = getReindex(originalTaskId, false);
        final long originalStartTimeMillis = initialResponse.getTaskResult().getTask().startTime();

        // ---- First relocation: A -> B ----
        assertFalse(".tasks should not exist before first shutdown", indexExists(TaskResultsService.TASK_INDEX));
        internalCluster().getInstance(ShutdownPrepareService.class, firstCoordinatorNodeName).prepareForShutdown();
        assertTrue(".tasks should be created after relocation", indexExists(TaskResultsService.TASK_INDEX));
        ensureGreen(TaskResultsService.TASK_INDEX);
        internalCluster().stopNode(firstCoordinatorNodeName);

        final GetReindexResponse midRelocationGet = getReindex(originalTaskId, false);
        final TaskInfo midInfo = midRelocationGet.getTaskResult().getTask();
        assertThat(midRelocationGet.getTaskResult().isCompleted(), is(false));
        assertThat(midInfo.taskId().getNodeId(), equalTo(secondCoordinatorNodeId));
        assertThat(midInfo.originalTaskId(), equalTo(originalTaskId));
        assertThat(midInfo.startTime(), equalTo(originalStartTimeMillis));
        final Map<String, Object> midMap = XContentTestUtils.convertToMap(midRelocationGet);
        assertThat(midMap.get("id"), equalTo(originalTaskId.toString()));
        assertThat(midMap.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        assertThat(midMap.get("original_task_id"), is(nullValue()));
        assertThat(midMap.get("original_start_time_in_millis"), is(nullValue()));

        // ---- Second relocation: B -> C ----
        // prepareForShutdown polls for up to the grace period (10s default), which covers the 5s relocation cooldown, which
        // prevents race conditions
        internalCluster().getInstance(ShutdownPrepareService.class, secondCoordinatorNodeName).prepareForShutdown();
        final long secondRelocationFinishedMillis = System.currentTimeMillis();
        assertThat(
            "second relocation should wait until at least 5s after the task started",
            secondRelocationFinishedMillis - originalStartTimeMillis,
            greaterThanOrEqualTo(TimeUnit.SECONDS.toMillis(5))
        );
        internalCluster().stopNode(secondCoordinatorNodeName);
        ensureStableCluster(1);

        // ---- Rethrottle ----
        final int newRps = requestsPerSecond * 2;
        final long timeMillisBeforeRethrottle = System.currentTimeMillis();
        final Map<String, Object> rethrottleBody = rethrottleReindex(originalTaskId, newRps);
        assertRethrottleHasTasksAndNoFailures(rethrottleBody, originalTaskId, originalStartTimeMillis, timeMillisBeforeRethrottle, newRps);

        // ---- List ----
        final List<Map<String, Object>> listedTasks = getRunningReindexes();
        assertThat(listedTasks, hasSize(1));
        final Map<String, Object> listedTask = listedTasks.getFirst();
        assertThat(listedTask.get("id"), equalTo(originalTaskId.toString()));
        assertThat(listedTask.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        assertThat(listedTask.get("original_task_id"), is(nullValue()));
        assertThat(listedTask.get("original_start_time_in_millis"), is(nullValue()));
        assertThat(ObjectPath.eval("status.requests_per_second", listedTask), closeTo(newRps, 0.001));

        // ---- Get (running) ----
        final GetReindexResponse getResponse = getReindex(originalTaskId, false);
        final TaskResult getResult = getResponse.getTaskResult();
        final TaskInfo getInfo = getResult.getTask();
        assertThat(getResult.isCompleted(), is(false));
        assertThat(getInfo.taskId().getNodeId(), equalTo(finalMasterDataNodeId));
        assertThat(getInfo.originalTaskId(), equalTo(originalTaskId));
        assertThat(getInfo.startTime(), equalTo(originalStartTimeMillis));
        final Map<String, Object> getMap = XContentTestUtils.convertToMap(getResponse);
        assertThat(ObjectPath.eval("status.requests_per_second", getMap), closeTo(newRps, 0.001));

        // ---- Cancel ----
        final long timeMillisBeforeCancel = System.currentTimeMillis();
        final CancelReindexResponse cancelResponse = client().execute(
            TransportCancelReindexAction.TYPE,
            new CancelReindexRequest(originalTaskId, true)
        ).actionGet(TimeValue.timeValueSeconds(60));
        final Map<String, Object> cancelBody = XContentTestUtils.convertToMap(cancelResponse);
        assertThat(cancelBody.get("cancelled"), is(true));
        assertThat(cancelBody.get("completed"), is(true));
        assertThat(cancelBody.get("id"), equalTo(originalTaskId.toString()));
        assertThat(cancelBody.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        // we have millisecond precision here, so leave space for 1ms because runningTimeInNanos has nanosecond resolution
        final long expectedMinimumCancelTimeMillis = TimeUnit.MILLISECONDS.toNanos(timeMillisBeforeCancel - originalStartTimeMillis - 1);
        assertThat(((Number) cancelBody.get("running_time_in_nanos")).longValue(), greaterThanOrEqualTo(expectedMinimumCancelTimeMillis));
        assertThat(cancelBody.get("original_task_id"), is(nullValue()));
        assertThat(cancelBody.get("original_start_time_in_millis"), is(nullValue()));
        assertThat(ObjectPath.eval("status.canceled", cancelBody), equalTo("by user request"));

        // ---- List after cancel ----
        assertBusy(() -> assertThat(getRunningReindexes(), hasSize(0)));

        // ---- Get after cancel ----
        final GetReindexResponse afterCancelGet = getReindex(originalTaskId, false);
        final Map<String, Object> afterCancelMap = XContentTestUtils.convertToMap(afterCancelGet);
        assertThat(afterCancelMap.get("completed"), is(true));
        assertThat(afterCancelMap.get("cancelled"), is(false));
        assertThat(afterCancelMap.get("id"), equalTo(originalTaskId.toString()));
        assertThat(afterCancelMap.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        assertThat(afterCancelMap.get("original_task_id"), is(nullValue()));
        assertThat(afterCancelMap.get("original_start_time_in_millis"), is(nullValue()));
        assertThat(ObjectPath.eval("status.canceled", afterCancelMap), equalTo("by user request"));
        assertThat(ObjectPath.eval("response.canceled", afterCancelMap), equalTo("by user request"));
    }

    private GetReindexResponse getReindex(final TaskId taskId, final boolean waitForCompletion) {
        return client().execute(
            TransportGetReindexAction.TYPE,
            new GetReindexRequest(taskId, waitForCompletion, TimeValue.timeValueSeconds(30))
        ).actionGet();
    }

    private List<Map<String, Object>> getRunningReindexes() throws IOException {
        final ListReindexResponse response = client().execute(TransportListReindexAction.TYPE, new ListReindexRequest(true)).actionGet();
        final Map<String, Object> responseMap = XContentTestUtils.convertToMap(response);
        return ObjectPath.eval("reindex", responseMap);
    }

    private Map<String, Object> rethrottleReindex(final TaskId taskId, final int requestsPerSecond) throws Exception {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
        request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
        final Response response = getRestClient().performRequest(request);
        return ESRestTestCase.entityAsMap(response);
    }

    private void assertRethrottleHasTasksAndNoFailures(
        final Map<String, Object> body,
        final TaskId originalTaskId,
        final long originalStartTimeMillis,
        final long timeMillisBeforeRethrottle,
        final int expectedRps
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
        assertThat("correct RPS", ObjectPath.eval("status.requests_per_second", soleTask), closeTo(expectedRps, 0.0001f));
        final List<Object> taskFailures = ObjectPath.eval("task_failures", body);
        final List<Object> nodeFailures = ObjectPath.eval("node_failures", body);
        if (taskFailures != null) {
            assertThat("rethrottle should have no task failures", taskFailures, empty());
        }
        if (nodeFailures != null) {
            assertThat("rethrottle should have no node failures", nodeFailures, empty());
        }
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

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
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
}
