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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.transport.MockTransportService;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class CancelTasksRelocationIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(CancelTasksRelocationIT.class);

    private static final String SOURCE_INDEX = "cancel_reindex_src";

    private static final String DEST_INDEX = "cancel_reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    // RPS takes slices and batch size into account to approximate ~1s per slice
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

    /**
     * Races a cancel against an in-progress relocation and asserts that after the race settles the task is in exactly one
     * consistent final state — cancelled xor relocated — and that no orphan task remains on either node afterward.
     * <p>
     * Which side wins depends on which CAS on {@code BulkByScrollTask.RelocationProgress} runs first:
     * <ul>
     *     <li><b>Cancel wins</b>: The source task finishes cancelled; no resumed task is created on the destination.</li>
     *     <li><b>Relocation wins</b>: The resumed task runs on the destination; the cancel is rejected.</li>
     * </ul>
     */
    public void testConcurrentCancelAndRelocationIsConsistent() throws Exception {
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

        // Kick off relocation (via node shutdown) and cancel (by original task id) concurrently, coordinated by a
        // single latch so the race is meaningful.
        final CountDownLatch raceStartLatch = new CountDownLatch(1);
        final AtomicReference<ActionFuture<ListTasksResponse>> cancelFutureRef = new AtomicReference<>();
        final Thread relocationThread = new Thread(() -> {
            awaitLatch(raceStartLatch);
            // prepareForShutdown requests relocation and blocks until the reindex finishes (either relocated or
            // cancelled via the CAS race).
            internalCluster().getInstance(ShutdownPrepareService.class, shutdownNode).prepareForShutdown();
        }, "relocation-thread");
        final Thread cancelThread = new Thread(() -> {
            awaitLatch(raceStartLatch);
            final CancelTasksRequest request = new CancelTasksRequest();
            request.setTargetTaskId(originalTaskId);
            cancelFutureRef.set(clusterAdmin().cancelTasks(request));
        }, "cancel-thread");
        relocationThread.start();
        cancelThread.start();
        raceStartLatch.countDown();
        relocationThread.join(TimeValue.timeValueMinutes(1).millis());
        cancelThread.join(TimeValue.timeValueMinutes(1).millis());

        // After the race settles, classify each side independently.
        final boolean cancelCommitted = cancelCommittedOriginalTask(cancelFutureRef.get(), originalTaskId);
        final TaskResult sourceResult = readSourceTaskResult(originalTaskId);
        final boolean relocationCommitted = isRelocatedResult(sourceResult);

        // Invariant: exactly one side committed. Both sides committing (double-commit) is the bug the CAS gate
        // prevents; neither committing would mean the task vanished silently.
        assertTrue(
            "task must have committed either cancellation or relocation but not both: "
                + "cancelCommitted="
                + cancelCommitted
                + ", relocationCommitted="
                + relocationCommitted
                + ", sourceResult="
                + sourceResult.getErrorAsMap(),
            cancelCommitted ^ relocationCommitted
        );

        // Orphan check: after the dust settles there must be no reindex task running anywhere. If relocation won,
        // unthrottle and cancel the resumed task so the heavily-throttled task doesn't linger into teardown.
        if (relocationCommitted) {
            final TaskId resumedTaskId = new TaskId((String) sourceResult.getErrorAsMap().get("relocated_task_id"));
            unthrottleReindex(resumedTaskId);
            final CancelTasksRequest cleanup = new CancelTasksRequest();
            cleanup.setTargetTaskId(resumedTaskId);
            cleanup.setWaitForCompletion(true);
            clusterAdmin().cancelTasks(cleanup).actionGet();
        }
        assertBusy(
            () -> assertThat("no orphan reindex task remains after the race has settled", listAllReindexTasks(), hasSize(0)),
            30,
            TimeUnit.SECONDS
        );

        internalCluster().stopNode(shutdownNode);
    }

    /**
     * Forces the relocation handoff to sit mid-flight so a cancel issued during that window reliably hits the CAS gate and is rejected
     * with {@code 409 CONFLICT}.
     * <p>
     * The destination node's transport is configured to hold any {@code ResumeReindexAction} message until we release
     * it. After {@code prepareForShutdown} triggers the handoff and the source has CAS'd its {@code RelocationProgress}
     * into {@code HANDOFF_INITIATED}, we observe the held message, fire the cancel, and only then release the hold so
     * relocation completes and the cluster can be torn down cleanly.
     */
    public void testCancelBailsWhenHandoffInitiated() throws Exception {
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

            // Fire the cancel: must bail with 409 because the source's RelocationProgress is HANDOFF_INITIATED.
            final CancelTasksRequest cancel = new CancelTasksRequest();
            cancel.setTargetTaskId(originalTaskId);
            final ListTasksResponse response = clusterAdmin().cancelTasks(cancel).actionGet(TimeValue.timeValueSeconds(30));
            assertThat(response.getTasks(), hasSize(0));
            assertThat(response.getTaskFailures(), hasSize(1));
            final Throwable cause = response.getTaskFailures().get(0).getCause();
            assertThat(cause, instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) cause).status(), is(RestStatus.CONFLICT));
            assertThat(cause.getMessage(), equalTo("cannot cancel task [" + originalTaskId.getId() + "] because it is being relocated"));
        } finally {
            // Release so the rest of the flow can unwind regardless of assertion outcome.
            releaseResume.countDown();
            // The relocationThread is the daemon we spawned; bound its join so the test can't hang here.
            relocationThread.join(TimeValue.timeValueMinutes(1).millis());
            // Best-effort teardown: cancel any lingering reindex tasks (resumed + rethrottle) so the heavy throttle
            // doesn't push the test past the suite timeout. Failures here are swallowed - the point of this test
            // is the 409 assertion above, not the cleanup.
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
     * Returns true iff the cancel API returned a response that includes the original task id with {@code cancelled=true}.
     */
    private static boolean cancelCommittedOriginalTask(ActionFuture<ListTasksResponse> cancelFuture, TaskId originalTaskId) {
        try {
            final ListTasksResponse response = cancelFuture.actionGet();
            return response.getTasks().stream().anyMatch(t -> originalTaskId.equals(t.taskId()) && t.cancelled());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns true iff the stored {@code .tasks} entry for the source task carries a {@code TaskRelocatedException}
     * error, which means the task is relocated.
     */
    private static boolean isRelocatedResult(TaskResult sourceResult) {
        final Map<String, Object> errorMap = sourceResult.getErrorAsMap();
        return errorMap != null && "task_relocated_exception".equals(errorMap.get("type"));
    }

    // -- helpers --

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

    private void unthrottleReindex(final TaskId taskId) throws IOException {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
        request.addParameter("requests_per_second", Integer.toString(-1));
        getRestClient().performRequest(request);
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

    private TaskResult readSourceTaskResult(TaskId originalTaskId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());
        final GetResponse getResponse = client().prepareGet(TaskResultsService.TASK_INDEX, originalTaskId.toString()).get();
        assertThat("source task must be recorded in .tasks after the race settles", getResponse.isExists(), is(true));
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, getResponse.getSourceAsString())
        ) {
            final TaskResult result = TaskResult.PARSER.apply(parser, null);
            assertThat("source task is completed in .tasks", result.isCompleted(), is(true));
            return result;
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
