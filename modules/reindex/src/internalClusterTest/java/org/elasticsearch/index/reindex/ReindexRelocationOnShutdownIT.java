/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.RethrottleRequestBuilder;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.reindex.management.ReindexManagementPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeShutdownTestUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.node.ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests reindex relocation on stateful clusters.
 * <p>
 * Reindex relocation works when a node containing a reindexing task shuts down. Providing that there is
 * another node capable of hosting the task, then the reindexing task will relocate without failure and resume
 * on the second node. Beneath the hood, reindexing uses point-in-time search to achieve this, since point-in-time
 * searches can be relocated. This relocation is <i>guaranteed</i> on serverless since there is a single object store
 * holding the lucene commits in a defined order, but this is not the case on stateful. Stateful relocations vary depending
 * on the cluster configuration. If a node shuts down in a stateful cluster holding a search shard, then the pit is forced to close too.
 * Stateful relocation also requires a target node not marked for shutdown. If the only candidate
 * (e.g. the sole data node) is already shutting down, the task cannot relocate.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexRelocationOnShutdownIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ReindexRelocationOnShutdownIT.class);

    private static final String SOURCE = "reindex-relocation-source";
    private static final String DEST = "reindex-relocation-dest";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    /**
     * Creates a 3 node cluster with a master node, data node and coordinating node (that will hold the reindexing request).
     * By shutting down the coordination node, the reindexing task is forced to relocate to the data node. Since the data node is not
     * shutting down, then pit relocation is guaranteed to succeed. We then assert that the destination index eventually contains
     * all documents.
     */
    public void testReindexTaskRelocatesOnNodeShutdown() throws Exception {
        assumeTrue("reindex resilience must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final Settings coordSettings = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(3);

        // Keep the doc size small so the task finishes quickly
        final int numDocs = randomIntBetween(10, 40);
        createIndex(SOURCE);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(SOURCE).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(SOURCE).setSize(0).setTrackTotalHits(true), numDocs);

        // Randomly make the source index read only
        if (randomBoolean()) {
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), SOURCE);
        }

        // Heavily throttle the request so that it can't complete before we shut the node down
        final ReindexRequest request = new ReindexRequest().setSourceIndices(SOURCE)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f);
        // This is the batch size of how many documents to return per search.
        request.getSearchRequest().source().size(1);

        // Start the reindexing task on the coordinating node
        final CountDownLatch listenerDone = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<BulkByScrollResponse> success = new AtomicReference<>();
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                success.set(bulkByScrollResponse);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });

        waitForRootReindexTask(coordNodeName);

        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);
        // This function only returns once all hooks have completed, including marking the reindexing task for relocation.
        // This triggers the reindexing task to relocate rather than continuing to reindex after it completes a page of reindexing
        // (and we have a page size of 1 so can guarantee that the task is not completed at this point). However, due to the
        // high throttle, we would have to wait for the between-pages sleep (which is a long time).
        shutdownPrepareService.prepareForShutdown();
        // Therefore, we rethrottle the reindexing task to run unlimited requests per second, immediately triggering relocation
        rethrottleRunningRootReindex(numDocs);
        internalCluster().stopNode(coordNodeName);

        assertTrue("reindex listener should complete", listenerDone.await(30, TimeUnit.SECONDS));

        // Assert that the reindexing task on the first node failed
        final Throwable error = failure.get();
        final BulkByScrollResponse response = success.get();
        assertThat(ExceptionsHelper.unwrapCause(error), instanceOf(TaskRelocatedException.class));
        final TaskRelocatedException relocated = (TaskRelocatedException) ExceptionsHelper.unwrapCause(error);
        final String relocatedTaskIdString = relocated.getRelocatedTaskId().orElseThrow();
        assertNull(response);

        // Wait until the relocated task finishes (including persisting to `.tasks` when shouldStoreResult is true)
        // so tear-down does not race with async task-result indexing.
        final GetTaskResponse relocatedTaskFinished = clusterAdmin().prepareGetTask(new TaskId(relocatedTaskIdString))
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get();
        assertTrue("relocated reindex should complete", relocatedTaskFinished.getTask().isCompleted());

        // Asserts that the reindexing task is relocated to another node and succeeds
        assertBusy(() -> {
            assertTrue(indexExists(DEST));
            flushAndRefresh(DEST);
            assertHitCount(prepareSearch(DEST).setSize(0).setTrackTotalHits(true), numDocs);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Pit relocation fails when a node shuts down containing at least one search shard used in the pit.
     * The reindexing relocation should succeed as there is a node free for the task to relocate to, but then
     * the reindexing task should immediately fail when attempting to use the old pit which <i>hasn't</i> relocated.
     * <P>
     * Two errors can occur during this:
     * <ol>
     *     <li>
     *         {@link org.elasticsearch.action.search.SearchContextMissingNodesException} is thrown when the
     *         reindexing task tries to search using a pit that could not relocate. In this case, the pit will have been closed
     *         by node 1 shutting down, but reindexing has not realised this yet, and so is still attempting to call it.
     *     </li>
     *     <li>
     *         The reindexing task on node 2 is started before node 1 is fully shut down (which mimics production since shutdown can
     *         take up to an hour). However, due to the set-up of the test, search shards required for the pit are
     *         present on node 1. Therefore, if the reindexing task on node 2 is trying to access these shards on node 1 and
     *         then node 1 shuts down, then we get a different error.
     *         These errors are {@link org.elasticsearch.action.search.SearchPhaseExecutionException} with
     *         {@code node_not_connected_exception} if the query phase contacts the stopped node during the transport teardown.
     *     </li>
     * </ol>
     */
    public void testReindexFailsWhenPitRelocationFails() throws Exception {
        assumeTrue("reindex resilience must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        internalCluster().startMasterOnlyNode();

        final Settings dataNodeWithShutdownTimeout = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(120))
            .build();
        final String dataNodeRunningReindex = internalCluster().startDataOnlyNode(dataNodeWithShutdownTimeout);
        internalCluster().startDataOnlyNode();

        ensureStableCluster(3);

        prepareCreate(SOURCE).setSettings(
            Settings.builder()
                // Forces one primary on each node, so that there is a primary on the node shutting down
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.total_shards_per_node", 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
        ).get();
        ensureGreen(SOURCE);

        prepareCreate(DEST).setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
        ).get();

        final int numDocs = randomIntBetween(10, 40);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(SOURCE).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(SOURCE).setSize(0).setTrackTotalHits(true), numDocs);

        // Create `.tasks` before reindex runs so its shards are allocated with the rest of the cluster, and set delayed
        // node-left timeout to zero. Otherwise, when we stop the data node that holds the `.tasks` primary, the default
        // ~1 minute delay before reallocating can cause task-result persistence to time out while the test polls GetTask.
        assertAcked(indicesAdmin().prepareCreate(TaskResultsService.TASK_INDEX));
        assertAcked(
            indicesAdmin().prepareUpdateSettings(TaskResultsService.TASK_INDEX)
                .setSettings(
                    Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO).build()
                )
                .origin(TransportGetTaskAction.TASKS_ORIGIN)
        );
        ensureGreen(TaskResultsService.TASK_INDEX);

        final ReindexRequest request = new ReindexRequest().setSourceIndices(SOURCE)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f);
        request.getSearchRequest().source().size(1);

        final CountDownLatch listenerDone = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<BulkByScrollResponse> success = new AtomicReference<>();

        internalCluster().client(dataNodeRunningReindex).execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                success.set(bulkByScrollResponse);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });

        waitForRootReindexTask(dataNodeRunningReindex);

        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(
            ShutdownPrepareService.class,
            dataNodeRunningReindex
        );
        // This function only returns once all hooks have completed, including marking the reindexing task for relocation.
        // This triggers the reindexing task to relocate rather than continuing to reindex after it completes a page of reindexing
        // (and we have a page size of 1 so can guarantee that the task is not completed at this point). However, due to the
        // high throttle, we would have to wait for the between-pages sleep (which is a long time).
        shutdownPrepareService.prepareForShutdown();
        // Therefore, we rethrottle the reindexing task to run unlimited requests per second, immediately triggering relocation
        rethrottleRunningRootReindex(numDocs);

        // We wait until the reindexing task has been relocated before actually stopping the node
        assertBusy(() -> {
            final Throwable f = failure.get();
            assertNotNull("reindex client should receive relocation before its node stops", f);
            assertThat(ExceptionsHelper.unwrapCause(f), instanceOf(TaskRelocatedException.class));
        }, 30, TimeUnit.SECONDS);
        final String stoppedDataNodeId = internalCluster().getInstance(ClusterService.class, dataNodeRunningReindex).localNode().getId();
        internalCluster().stopNode(dataNodeRunningReindex);

        assertTrue("reindex listener should complete", listenerDone.await(30, TimeUnit.SECONDS));

        // Assert on the original task which has been relocated
        final Throwable error = failure.get();
        assertThat(ExceptionsHelper.unwrapCause(error), instanceOf(TaskRelocatedException.class));
        final String relocatedTaskIdString = ((TaskRelocatedException) ExceptionsHelper.unwrapCause(error)).getRelocatedTaskId()
            .orElseThrow();
        assertNull(success.get());

        // Assert on the relocated task
        assertBusy(() -> {
            final GetTaskResponse relocatedTaskFinished = clusterAdmin().prepareGetTask(new TaskId(relocatedTaskIdString))
                .setWaitForCompletion(false)
                .get();
            final TaskResult relocated = relocatedTaskFinished.getTask();
            assertTrue("relocated reindex should finish", relocated.isCompleted());
            assertTrue(
                "relocated reindex should fail with PIT missing-nodes or search-phase disconnect after stopped node",
                taskResultIndicatesRelocatedReindexFailedAfterNodeLeft(relocated, stoppedDataNodeId)
            );
        }, 120, TimeUnit.SECONDS);
    }

    /**
     * Creates a cluster with a sole data node marked for removal. Since there is no node available for the task to be relocated to,
     * it attempts to complete the reindexing task before the node shuts down. In this test, the task cannot complete in time
     * and so an error is returned to the client
     */
    public void testReindexTaskFailsWhenDataNodeIsShuttingDownAndTaskDoesNotFinishInTime() throws Exception {
        assumeTrue("reindex resilience must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        final String masterNodeName = internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final Settings coordSettings = Settings.builder()
            /*
             * On the coordinating node, {@link ShutdownPrepareService} registers several shutdown hooks that run in parallel
             * (each on its own thread). {@code prepareForShutdown()} waits until all of them finish (or until an overall time limit is
             * hit). Waiting for task completion is one of these hooks, specifically waiting on the throttled reindexing task. If
             * {@link #MAXIMUM_REINDEXING_TIMEOUT_SETTING} is not set to something small like 2 seconds, then it would wait for the entire
             * task to finish. Setting the limit to be 2 seconds + heavy throttling guarantees that the task cannot succeed before shutdown
             * without making the test run too long, and gives enough time for the hook to mark the tasks for relocation
             */
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(3);

        final int numDocs = randomIntBetween(10, 40);
        createIndex(SOURCE);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(SOURCE).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(SOURCE).setSize(0).setTrackTotalHits(true), numDocs);

        final ReindexRequest request = new ReindexRequest().setSourceIndices(SOURCE)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f);
        request.getSearchRequest().source().size(1);

        // Start the reindexing task on the coordinating node
        final CountDownLatch listenerDone = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<BulkByScrollResponse> success = new AtomicReference<>();
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                success.set(bulkByScrollResponse);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });

        waitForRootReindexTask(coordNodeName);

        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        NodeShutdownTestUtils.putShutdownForRemovalMetadata(dataNodeName, clusterService);

        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);
        shutdownPrepareService.prepareForShutdown();
        // Forcibly shutting the node before the reindexing task completes
        internalCluster().stopNode(coordNodeName);

        assertTrue("reindex listener should complete", listenerDone.await(30, TimeUnit.SECONDS));

        final Throwable error = failure.get();
        final BulkByScrollResponse response = success.get();
        assertTrue(
            "reindex should surface coordinator shutdown as a transport failure or as bulk failures on the response",
            reindexClientIndicatesCoordinatingNodeClosed(error, response)
        );

        // Since the node holding the reindexing task is shut down, the reindexing workflow never runs the cleanup wrapper
        // to close the open pit context. Manually closing the data node avoids the {@code ESIntegTestCase#after()}
        // check failing for leaked search contexts
        internalCluster().stopNode(dataNodeName);
    }

    /**
     * Creates a cluster with a sole data node marked for removal. Since there is no node available for the task to be relocated to,
     * the task attempts to complete before node shutdown. Here we allow the test to complete and expect no errors
     */
    public void testReindexTaskFinishesBeforeNodeShutsDown() throws Exception {
        assumeTrue("reindex resilience must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        final String masterNodeName = internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final Settings coordSettings = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(3);

        final int numDocs = randomIntBetween(10, 40);
        createIndex(SOURCE);
        createIndex(DEST, indexSettings(1, 0).build());
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(SOURCE).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(SOURCE).setSize(0).setTrackTotalHits(true), numDocs);

        final ReindexRequest request = new ReindexRequest().setSourceIndices(SOURCE)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f);
        request.getSearchRequest().source().size(1);

        // Start the reindexing task
        final CountDownLatch listenerDone = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<BulkByScrollResponse> success = new AtomicReference<>();
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                success.set(bulkByScrollResponse);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });

        waitForRootReindexTask(coordNodeName);

        // Shut down the data node so the task will not be able to relocate
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        NodeShutdownTestUtils.putShutdownForRemovalMetadata(dataNodeName, clusterService);
        // Remove the heavy rethrottle so the task can complete in time
        rethrottleRunningRootReindex(numDocs);
        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);
        shutdownPrepareService.prepareForShutdown();

        assertTrue("reindex listener should complete", listenerDone.await(60, TimeUnit.SECONDS));

        assertNull(failure.get());
        final BulkByScrollResponse response = success.get();
        assertNotNull(response);
        assertTrue(response.getBulkFailures().isEmpty());
        assertTrue(response.getSearchFailures().isEmpty());
        assertBusy(() -> {
            assertTrue(indexExists(DEST));
            flushAndRefresh(DEST);
            assertHitCount(prepareSearch(DEST).setSize(0).setTrackTotalHits(true), numDocs);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * When the coordinating node stops mid-reindex, the client may get {@link ActionListener#onFailure} with
     * {@link NodeClosedException}, or {@link ActionListener#onResponse} with a {@link BulkByScrollResponse} whose
     * {@link BulkByScrollResponse#getBulkFailures()} wrap {@link NodeClosedException} after bulk indexing hits a closing node.
     */
    private static boolean reindexClientIndicatesCoordinatingNodeClosed(
        final Throwable clientFailure,
        final BulkByScrollResponse response
    ) {
        if (clientFailure != null && ExceptionsHelper.unwrapCause(clientFailure) instanceof NodeClosedException) {
            return true;
        }
        if (response != null) {
            for (BulkItemResponse.Failure bulkFailure : response.getBulkFailures()) {
                if (ExceptionsHelper.unwrapCause(bulkFailure.getCause()) instanceof NodeClosedException) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * After the data node that held part of the PIT is stopped, the relocated reindex may record either:
     * <ul>
     *   <li>
     *       {@code search_context_missing_nodes_exception} (top-level or under bulk {@code failures}) which are thrown when the PIT is
     *       deemed invalid because a data node containing at least one shard has shut down, or
     *   </li>
     *   <li>
     *       {@code search_phase_execution_exception} whose {@code failed_shards} for {@code stoppedDataNodeId} wrap
     *       {@code node_not_connected_exception} or {@code node_disconnected_exception} (from
     *       {@link org.elasticsearch.transport.NodeNotConnectedException} or
     *       {@link org.elasticsearch.transport.NodeDisconnectedException}), which appear when a normal search phase still routes or
     *       sends work to the old node while the connection is already gone.
     *   </li>
     * </ul>
     */
    private static boolean taskResultIndicatesRelocatedReindexFailedAfterNodeLeft(
        final TaskResult taskResult,
        final String stoppedDataNodeId
    ) {
        if (matchesSearchContextMissingNodesFailure(taskResult)) {
            return true;
        }
        if (matchesSearchPhaseNodeDisconnectedFromStoppedNode(taskResult, stoppedDataNodeId)) {
            return true;
        }
        logger.warn(
            "relocated reindex task result did not match expected failure "
                + "(search_context_missing_nodes_exception or search_phase_execution with node_not_connected/node_disconnected for [{}]): "
                + "completed=[{}] task=[{}] error=[{}] responseKeys=[{}]",
            stoppedDataNodeId,
            taskResult.isCompleted(),
            taskResult.getTask(),
            taskResult.getError(),
            taskResult.getResponseAsMap().keySet()
        );
        return false;
    }

    /** Top-level {@code search_context_missing_nodes_exception}, or same type nested under bulk {@code failures}. */
    private static boolean matchesSearchContextMissingNodesFailure(final TaskResult taskResult) {
        if (taskResult.getError() != null) {
            final Map<String, Object> errorMap = taskResult.getErrorAsMap();
            final Object errorType = errorMap != null ? errorMap.get("type") : null;
            return "search_context_missing_nodes_exception".equals(errorType);
        }
        final Map<String, Object> responseMap = taskResult.getResponseAsMap();
        final Object failuresObj = responseMap.get(BulkByScrollResponse.FAILURES_FIELD);
        if (failuresObj instanceof List<?> failureList) {
            for (Object entry : failureList) {
                if (entry instanceof Map<?, ?> failureMap) {
                    final Object reason = failureMap.get("reason");
                    if (reason instanceof Map<?, ?> reasonMap && "search_context_missing_nodes_exception".equals(reasonMap.get("type"))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * {@link org.elasticsearch.action.search.SearchPhaseExecutionException} with a shard failure on the stopped node
     * due to {@link org.elasticsearch.transport.NodeNotConnectedException} or
     * {@link org.elasticsearch.transport.NodeDisconnectedException}.
     */
    private static boolean matchesSearchPhaseNodeDisconnectedFromStoppedNode(final TaskResult taskResult, final String stoppedDataNodeId) {
        if (taskResult.getError() == null) {
            return false;
        }
        final Map<String, Object> errorMap = taskResult.getErrorAsMap();
        if (errorMap == null || "search_phase_execution_exception".equals(errorMap.get("type")) == false) {
            return false;
        }
        final Object failedShardsObj = errorMap.get("failed_shards");
        if (failedShardsObj instanceof List<?> failedShards) {
            for (Object fs : failedShards) {
                if (fs instanceof Map<?, ?> shardFailure) {
                    if (stoppedDataNodeId.equals(shardFailure.get("node"))
                        && reasonMapIndicatesNodeTransportDisconnect(shardFailure.get("reason"))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * True if the serialized failure is a transport disconnect to the target node: either {@code node_not_connected_exception}
     * or {@code node_disconnected_exception} (including under {@code caused_by}).
     */
    private static boolean reasonMapIndicatesNodeTransportDisconnect(final Object reasonObj) {
        if (reasonObj instanceof Map<?, ?> reason) {
            if (isNodeTransportDisconnectExceptionType(reason.get("type"))) {
                return true;
            }
            final Object causedBy = reason.get("caused_by");
            if (causedBy instanceof Map<?, ?> cb && isNodeTransportDisconnectExceptionType(cb.get("type"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNodeTransportDisconnectExceptionType(final Object type) {
        return "node_not_connected_exception".equals(type) || "node_disconnected_exception".equals(type);
    }

    /**
     * Waits until a top-level (non-child) reindex task is visible on the given node’s task list,
     * so shutdown/relocation runs against an actually running task.
     */
    private void waitForRootReindexTask(String nodeName) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks(nodeName)
                .setActions(ReindexAction.INSTANCE.name())
                .setDetailed(true)
                .get();
            tasks.rethrowFailures("find reindex task");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                if (taskInfo.parentTaskId().isSet() == false) {
                    return;
                }
            }
            fail("no root reindex task on [" + nodeName + "]: " + tasks.getTasks());
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * After relocation the resumed task keeps the original requests_per_second; set it unbounded so assertBusy on dest can finish.
     */
    private void rethrottleRunningRootReindex(int numDocs) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks().setActions(ReindexAction.INSTANCE.name()).setDetailed(true).get();
            tasks.rethrowFailures("list reindex tasks for rethrottle");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                if (taskInfo.parentTaskId().isSet()) {
                    continue;
                }
                try {
                    ListTasksResponse rethrottleResponse = new RethrottleRequestBuilder(client()).setTargetTaskId(taskInfo.taskId())
                        // Forces the reindexing task to still take 2 seconds, giving enough time for the node to shut down
                        .setRequestsPerSecond((float) numDocs / 2)
                        // Follow the relocation chain: if the task relocated between listing and rethrottling,
                        // the rethrottle would silently return an empty success without this flag set.
                        .setFollowRelocations(true)
                        .get();
                    rethrottleResponse.rethrowFailures("rethrottle after relocation");
                    return;
                } catch (ElasticsearchException e) {
                    Throwable unwrapped = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
                    if (unwrapped == null) {
                        throw e;
                    }
                    assertThat(
                        unwrapped.getMessage(),
                        equalTo(
                            "task ["
                                + taskInfo.taskId().getId()
                                + "] has not yet been initialized to the point where it knows how to rethrottle itself"
                        )
                    );
                    throw new AssertionError("rethrottle before task ready", e);
                }
            }
            throw new AssertionError("no root reindex task found for rethrottle after relocation");
        }, 30, TimeUnit.SECONDS);
    }
}
