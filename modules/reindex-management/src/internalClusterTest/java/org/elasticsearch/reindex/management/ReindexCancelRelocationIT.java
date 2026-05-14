/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
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
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexCancelRelocationIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ReindexCancelRelocationIT.class);

    private static final String SOURCE_INDEX = "cancel_reindex_src";
    private static final String DEST_INDEX = "cancel_reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    // RPS keeps each slice well under 1s so the reindex doesn't sleep through relocation
    private final int requestsPerSecond = randomIntBetween(bulkSize * numOfSlices, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

    @BeforeClass
    public static void skipSetupIfReindexResilienceDisabled() {
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

    /// Pins the relocation handoff in `HANDOFF_INITIATED` so a cancel-reindex issued during that window reliably hits the CAS gate
    /// and is rejected with `503 SERVICE_UNAVAILABLE`, with the cause's status preserved through the cancel-reindex wrapper.
    public void testCancelReindexBailsWhenHandoffInitiated() throws Exception {
        final String indexHostNode = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String shutdownNode = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, indexHostNode);
        createIndexPinnedToNodeName(DEST_INDEX, indexHostNode);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(shutdownNode);

        // Hold ResumeReindexAction on the destination until we say go, so the source parks in HANDOFF_INITIATED.
        // The hold is deferred to a background thread so we don't block a transport handler thread, which can
        // otherwise cascade into starvation of unrelated requests to the destination node.
        final CountDownLatch resumeReceivedOnDestination = new CountDownLatch(1);
        final CountDownLatch releaseResume = new CountDownLatch(1);
        final MockTransportService destinationTransport = MockTransportService.getInstance(indexHostNode);
        destinationTransport.addRequestHandlingBehavior(ResumeReindexAction.NAME, (handler, request, channel, task) -> {
            resumeReceivedOnDestination.countDown();
            final Thread forwarder = new Thread(() -> {
                try {
                    releaseResume.await();
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception ignored) {
                        // channel already closed
                    }
                }
            }, "resume-reindex-hold");
            forwarder.setDaemon(true);
            forwarder.start();
        });

        final Thread relocationThread = new Thread(
            () -> internalCluster().getInstance(ShutdownPrepareService.class, shutdownNode).prepareForShutdown(),
            "relocation-thread"
        );
        relocationThread.start();

        try {
            // Wait for the handoff to reach the destination, i.e. the source has CAS'd into HANDOFF_INITIATED.
            assertTrue("relocation handoff must reach the destination within 60s", resumeReceivedOnDestination.await(60, TimeUnit.SECONDS));

            // Drive the cancel through the cancel-reindex API. This must surface the 503 service-unavailable raised by
            // BulkByPaginatedSearchTask.ensureCancellable() rather than swallowing it as a not-found, regardless of the underlying
            // delegation.
            final ElasticsearchException cancelReindexFailure = expectThrows(
                ElasticsearchException.class,
                () -> client().execute(TransportCancelReindexAction.TYPE, new CancelReindexRequest(originalTaskId, false))
                    .actionGet(TimeValue.timeValueSeconds(30))
            );

            assertThat(
                "cancel-reindex must propagate the 503 status from cancel-tasks via the wrapped cause",
                ExceptionsHelper.status(cancelReindexFailure),
                is(RestStatus.SERVICE_UNAVAILABLE)
            );

            final Throwable rootCause = ExceptionsHelper.unwrap(cancelReindexFailure, ElasticsearchStatusException.class);
            assertThat("relocation race cause is reachable on the cause chain", rootCause, is(notNullValue()));
            assertThat(((ElasticsearchStatusException) rootCause).status(), is(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                rootCause.getMessage(),
                equalTo("cannot cancel task [" + originalTaskId.getId() + "] because it is being relocated")
            );
        } finally {
            // Release so the rest of the flow can unwind regardless of assertion outcome.
            releaseResume.countDown();
            relocationThread.join(TimeValue.timeValueMinutes(1).millis());
            // Best-effort teardown: cancel any lingering reindex tasks (resumed + rethrottle) so the heavy throttle
            // doesn't push the test past the suite timeout.
            try {
                final CancelTasksRequest sweep = new CancelTasksRequest();
                sweep.setActions(ReindexAction.NAME);
                sweep.setWaitForCompletion(false);
                clusterAdmin().cancelTasks(sweep).actionGet(TimeValue.timeValueSeconds(30));
                for (TaskInfo t : listAllReindexTasks()) {
                    unthrottleReindex(t.taskId());
                }
                assertBusy(() -> assertThat(listAllReindexTasks(), hasSize(0)), 60, TimeUnit.SECONDS);
            } catch (Exception teardownFailure) {
                logger.warn("teardown best-effort failed (ignored)", teardownFailure);
            }
            internalCluster().stopNode(shutdownNode);
        }
    }

    /**
     * Verifies that {@code POST /_reindex/{taskId}/_cancel} cancels a reindex task using its <em>original</em> task id even after
     * relocation has moved the task to a new node and the original host has left the cluster.
     */
    public void testCancelReindexCancelsRelocatedTaskByOriginalTaskId() throws Exception {
        final String indexHostNode = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String shutdownNode = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, indexHostNode);
        createIndexPinnedToNodeName(DEST_INDEX, indexHostNode);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(shutdownNode);
        assertThat("original task should be on the to-be-shutdown node", originalTaskId.getNodeId(), equalTo(nodeIdByName(shutdownNode)));

        final long originalStartTimeMillis = client().execute(
            TransportGetReindexAction.TYPE,
            new GetReindexRequest(originalTaskId, false, TimeValue.timeValueSeconds(30))
        ).actionGet().getTaskResult().getTask().startTime();

        // Trigger relocation; this blocks until the source has handed off to the destination and the .tasks index records the relocation.
        internalCluster().getInstance(ShutdownPrepareService.class, shutdownNode).prepareForShutdown();
        ensureGreen(TaskResultsService.TASK_INDEX);

        // After relocation, the original task id is recorded in .tasks with a relocated_task_id pointing to the destination.
        final TaskId relocatedTaskId = readRelocatedTaskIdFromTasksIndex(originalTaskId);
        assertThat("task should have relocated to the host node", relocatedTaskId.getNodeId(), equalTo(nodeIdByName(indexHostNode)));

        // Stop the original node so its node id is no longer in the cluster state — the cancel below has to rely on originalTaskId
        // matching to find the resumed task on the destination.
        internalCluster().stopNode(shutdownNode);
        ensureStableCluster(1);
        assertThat("relocated parent task should still be running on the destination", listReindexParentTasks(), hasSize(1));

        // Speed up the resumed task so the synchronous cancel doesn't time out waiting for the throttled work to finish cancelling.
        unthrottleReindex(relocatedTaskId);

        // Cancel via the original (stale) task id — this exercises cancel-tasks's originalTaskId match end-to-end.
        final long timeMillisBeforeCancel = System.currentTimeMillis();
        final CancelReindexResponse cancelResponse = client().execute(
            TransportCancelReindexAction.TYPE,
            new CancelReindexRequest(originalTaskId, true)
        ).actionGet(TimeValue.timeValueSeconds(60));

        final Map<String, Object> body = XContentTestUtils.convertToMap(cancelResponse);
        assertThat(
            "cancelled response embeds a completed GetReindexResponse",
            body,
            allOf(Matchers.hasEntry("cancelled", true), Matchers.hasEntry("completed", true))
        );
        assertThat(body.get("id"), equalTo(originalTaskId.toString()));
        assertThat(body.get("start_time_in_millis"), equalTo(originalStartTimeMillis));
        // we have millisecond precision here, so leave space for 1ms because runningTimeInNanos has nanosecond resolution
        final long expectedMinimumRunningTimeMillis = TimeUnit.MILLISECONDS.toNanos(timeMillisBeforeCancel - originalStartTimeMillis - 1);
        assertThat(((Number) body.get("running_time_in_nanos")).longValue(), greaterThanOrEqualTo(expectedMinimumRunningTimeMillis));
        assertThat(body.get("original_task_id"), is(nullValue()));
        assertThat(body.get("original_start_time_in_millis"), is(nullValue()));
        @SuppressWarnings("unchecked")
        final Map<String, Object> status = (Map<String, Object>) body.get("status");
        assertThat("task was cancelled by user request", status, Matchers.hasEntry("canceled", "by user request"));

        assertBusy(() -> assertThat("no reindex task should remain after cancellation", listReindexParentTasks(), hasSize(0)));
    }

    private List<TaskInfo> listReindexParentTasks() {
        return listAllReindexTasks().stream().filter(info -> info.parentTaskId().isSet() == false).toList();
    }

    /**
     * Reads the relocation marker for {@code originalTaskId} from the {@code .tasks} index and returns the relocated task id.
     */
    private TaskId readRelocatedTaskIdFromTasksIndex(final TaskId originalTaskId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());

        final GetResponse response = client().prepareGet(TaskResultsService.TASK_INDEX, originalTaskId.toString()).get();
        assertThat("original task should be recorded in .tasks", response.isExists(), is(true));
        final TaskResult result;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getSourceAsString())
        ) {
            result = TaskResult.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }
        assertThat("original task should be marked completed (relocated)", result.isCompleted(), is(true));
        final Map<String, Object> errorMap = result.getErrorAsMap();
        assertThat("error map carries the relocation marker", errorMap.get("type"), equalTo("task_relocated_exception"));
        final String relocatedId = (String) errorMap.get("relocated_task_id");
        assertThat("relocated_task_id is present", relocatedId, is(notNullValue()));
        return new TaskId(relocatedId);
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

    private List<TaskInfo> listAllReindexTasks() {
        final ListTasksResponse response = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        assertThat(response.getTaskFailures(), hasSize(0));
        assertThat(response.getNodeFailures(), hasSize(0));
        return response.getTasks();
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

    private void unthrottleReindex(final TaskId taskId) {
        try {
            final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
            request.addParameter("requests_per_second", Integer.toString(-1));
            getRestClient().performRequest(request);
        } catch (Exception e) {
            throw new AssertionError("failed to rethrottle reindex", e);
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

}
