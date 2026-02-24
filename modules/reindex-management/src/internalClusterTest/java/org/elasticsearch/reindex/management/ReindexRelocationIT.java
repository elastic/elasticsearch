/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration test(s) for reindex task relocation on node shutdown.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class ReindexRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";
    private static final int BULK_SIZE = 1;
    private static final int REQUESTS_PER_SECOND = 1;
    private static final int NUM_OF_SLICES = 1;
    private static final int NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST = 60 * REQUESTS_PER_SECOND * BULK_SIZE;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    /**
     * Test long-running non-sliced reindex task is relocated to a suitable node by doing the following:
     * 1. Create two data nodes: nodeA (hosting source and destination indices) and nodeB (hosting the reindex task)
     * 2. Create the source index pinned to nodeA without replicas, so the scroll always lives there
     * 3. Create the destination index pinned to nodeA without replicas, so it's available when we shutdown nodeB
     * 4. Start a throttled reindex on nodeB
     * 5. Stop nodeB and observe relocation to nodeA
     */
    public void testNonSlicedReindexRelocation() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

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
        indexRandom(true, SOURCE_INDEX, NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        // Start throttled async reindex on nodeB and check it has the expected state
        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(nodeBName);
        final TaskResult originalReindex = getRunningReindex(originalTaskId);
        assertThat("reindex should start on nodeB", originalReindex.getTask().taskId().getNodeId(), equalTo(nodeBId));
        assertRunningReindexTaskExpectedState(originalReindex.getTask());

        shutdownNodeNameAndRelocate(nodeBName);

        // Assert the original task is in .tasks index and has expected content (including relocated taskId on nodeA)
        final TaskId relocatedTaskId = assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(originalTaskId, nodeAId);

        // Assert relocated reindex is running and has expected state
        final TaskResult relocatedReindex = getRunningReindex(relocatedTaskId);
        assertThat("relocated reindex should be on nodeA", relocatedReindex.getTask().taskId().getNodeId(), equalTo(nodeAId));
        assertRunningReindexTaskExpectedState(relocatedReindex.getTask());

        // Speed up reindex post-relocation to keep the test fast
        rethrottleReindex(relocatedTaskId, -1); // unlimited

        assertRelocatedTaskExpectedEndState(relocatedTaskId);

        // assert all documents have been reindexed
        assertDocCount(DEST_INDEX, NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST);
    }
    // todo(szy): add test for two reindex task hops
    // todo(szy): add test for remote reindex

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

    private TaskId assertOriginalTaskExpectedEndStateAndGetRelocatedTaskId(
        final TaskResult originalResult,
        final TaskId originalTaskId,
        final String relocatedNodeId
    ) {
        assertThat("task completed", originalResult.isCompleted(), is(true));

        final Map<String, Object> innerResponse = originalResult.getResponseAsMap();
        assertThat(innerResponse, equalTo(Map.of()));

        final TaskInfo taskInfo = originalResult.getTask();
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), equalTo(Strings.format("reindex from [%s] to [%s]", SOURCE_INDEX, DEST_INDEX)));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));

        final Map<String, Object> taskStatus = ((RawTaskStatus) taskInfo.status()).toMap();
        assertThat(taskStatus.get("slice_id"), is(nullValue()));
        assertThat((Integer) taskStatus.get("total"), lessThanOrEqualTo(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("updated"), is(0));
        assertThat((Integer) taskStatus.get("created"), lessThan(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("deleted"), is(0));
        assertThat((Integer) taskStatus.get("batches"), lessThan(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("version_conflicts"), is(0));
        assertThat(taskStatus.get("noops"), is(0));
        assertThat(ObjectPath.eval("retries.bulk", taskStatus), is(0));
        assertThat(ObjectPath.eval("retries.search", taskStatus), is(0));
        assertThat((Integer) taskStatus.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat(taskStatus.get("requests_per_second"), is(1.0));
        assertThat(taskStatus.get("reason_cancelled"), is(nullValue()));
        assertThat((Integer) taskStatus.get("throttled_until_millis"), greaterThanOrEqualTo(0));

        final Map<String, Object> errorMap = originalResult.getErrorAsMap();
        assertThat(errorMap, is(aMapWithSize(4)));
        assertThat("we get expected error type", errorMap.get("type"), equalTo("task_relocated_exception"));
        assertThat("we get expected error reason", errorMap.get("reason"), equalTo("Task was relocated"));
        assertThat("we get expected original task id", errorMap.get("original_task_id"), equalTo(originalTaskId.toString()));
        final String relocatedTaskId = (String) errorMap.get("relocated_task_id");
        assertThat("we relocate to expected node", relocatedTaskId, startsWith(relocatedNodeId));
        return new TaskId(relocatedTaskId);
    }

    private void assertRelocatedTaskExpectedEndState(final TaskId taskId) throws Exception {
        final SetOnce<TaskResult> finishedResult = new SetOnce<>();

        assertBusy(() -> finishedResult.set(getCompletedTaskResult(taskId)), 30, TimeUnit.SECONDS);
        final TaskResult result = finishedResult.get();
        assertThat("relocated task has no error", result.getError(), is(nullValue()));
        final Map<String, Object> innerResponse = result.getResponseAsMap();
        assertThat(innerResponse.get("timed_out"), is(false));
        assertThat(innerResponse.get("total"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(innerResponse.get("updated"), is(0));
        assertThat(innerResponse.get("created"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(innerResponse.get("deleted"), is(0));
        assertThat(innerResponse.get("batches"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(innerResponse.get("version_conflicts"), is(0));
        assertThat(innerResponse.get("noops"), is(0));
        assertThat((Integer) innerResponse.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat(innerResponse.get("requests_per_second"), is(-1.0));
        assertThat((Integer) innerResponse.get("throttled_until_millis"), greaterThanOrEqualTo(0));
        assertThat(innerResponse.get("failures"), is(List.of()));

        final TaskInfo taskInfo = result.getTask();
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), equalTo(Strings.format("reindex from [%s] to [%s]", SOURCE_INDEX, DEST_INDEX)));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));

        final Map<String, Object> taskStatus = ((RawTaskStatus) taskInfo.status()).toMap();
        assertThat(taskStatus.get("slice_id"), is(nullValue()));
        assertThat(taskStatus.get("total"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("updated"), is(0));
        assertThat(taskStatus.get("created"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("deleted"), is(0));
        assertThat(taskStatus.get("batches"), is(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.get("version_conflicts"), is(0));
        assertThat(taskStatus.get("noops"), is(0));
        assertThat(ObjectPath.eval("retries.bulk", taskStatus), is(0));
        assertThat(ObjectPath.eval("retries.search", taskStatus), is(0));
        assertThat((Integer) taskStatus.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat(taskStatus.get("requests_per_second"), is(-1.0));
        assertThat(taskStatus.get("reason_cancelled"), is(nullValue()));
        assertThat((Integer) taskStatus.get("throttled_until_millis"), greaterThanOrEqualTo(0));
    }

    private TaskId assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(final TaskId taskId, final String relocatedNodeId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX); // replicas won't be allocated
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());
        final GetResponse getTaskResponse = client().prepareGet(TaskResultsService.TASK_INDEX, taskId.toString()).get();
        assertThat("task exists in .tasks index", getTaskResponse.isExists(), is(true));

        final TaskResult result;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, getTaskResponse.getSourceAsString())
        ) {
            result = TaskResult.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }

        return assertOriginalTaskExpectedEndStateAndGetRelocatedTaskId(result, taskId, relocatedNodeId);
    }

    private TaskId startAsyncThrottledReindexOnNode(final String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
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
                }
                """, SOURCE_INDEX, BULK_SIZE, DEST_INDEX));

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private TaskResult getRunningReindex(final TaskId taskId) {
        final TaskResult reindex = clusterAdmin().prepareGetTask(taskId).get().getTask();
        assertThat("reindex is running", reindex.isCompleted(), is(false));
        return reindex;
    }

    private void assertRunningReindexTaskExpectedState(final TaskInfo taskInfo) {
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), equalTo(Strings.format("reindex from [%s] to [%s]", SOURCE_INDEX, DEST_INDEX)));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));

        final BulkByScrollTask.Status taskStatus = ((BulkByScrollTask.Status) taskInfo.status());
        // lessThan because the initial running reindex might have "uninitialized" 0
        assertThat(taskStatus.getTotal(), lessThanOrEqualTo((long) NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.getUpdated(), is(0L));
        assertThat(taskStatus.getCreated(), lessThan((long) NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.getDeleted(), is(0L));
        assertThat(taskStatus.getBatches(), lessThan(NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST));
        assertThat(taskStatus.getVersionConflicts(), is(0L));
        assertThat(taskStatus.getNoops(), is(0L));
        assertThat(taskStatus.getBulkRetries(), is(0L));
        assertThat(taskStatus.getSearchRetries(), is(0L));
        assertThat(taskStatus.getThrottled(), greaterThanOrEqualTo(TimeValue.ZERO));
        assertThat(taskStatus.getRequestsPerSecond(), is(1.0f));
        assertThat(taskStatus.getReasonCancelled(), is(nullValue()));
        assertThat(taskStatus.getThrottledUntil(), greaterThanOrEqualTo(TimeValue.ZERO));
    }

    private TaskResult getCompletedTaskResult(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(true));
        return task;
    }

    private void createIndexPinnedToNodeName(final String index, final String nodeName) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), SOURCE_INDEX);
    }

    private void rethrottleReindex(final TaskId taskId, final int rps) {
        try {
            final RestClient restClient = getRestClient();
            final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
            request.addParameter("requests_per_second", Integer.toString(rps));
            restClient.performRequest(request);
        } catch (Exception e) {
            throw new AssertionError("failed to rethrottle reindex", e);
        }
    }

    private String nodeIdByName(final String nodeName) {
        final String nodeWithName = clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findAny()
            .orElse(null);
        assertNotNull("node with name not found ", nodeWithName);
        return nodeWithName;
    }

    private void assertDocCount(final String index, final int expected) throws IOException {
        assertNoFailures(indicesAdmin().prepareRefresh(index).get());
        final Request request = new Request("GET", "/" + index + "/_count");
        final Response response = getRestClient().performRequest(request);
        final Map<?, ?> body = ESRestTestCase.entityAsMap(response);
        final int count = ((Number) body.get("count")).intValue();
        assertThat(count, equalTo(expected));
    }
}
