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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.ReindexMetrics;
import org.elasticsearch.reindex.ReindexMetrics.SlicingMode;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration test(s) for testing a long-running reindex task is relocated to a suitable node on shutdown.
 * Checks expected state at each task phase: initial running, initial relocated, relocated running, and relocated finished.
 * <p>
 * Each test follows this flow:
 * 1. Create two data nodes: nodeA (hosting source and destination indices) and nodeB (hosting the reindex task)
 * 2. Create the source index pinned to nodeA without replicas, so the scroll always lives there
 * 3. Create the destination index pinned to nodeA without replicas, so it's available when we shutdown nodeB
 * 4. Start a throttled reindex on nodeB (depending on test: local sliced or unsliced, or non-sliced remote)
 * 5. Stop nodeB and observe relocation to nodeA
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    // make sure any one slice doesn't sleep longer than shutdown timeout (10s); with this, each slice will at most sleep for 4s
    private final int requestsPerSecond = randomIntBetween(bulkSize, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

    @Before
    public void resetPlugin() {
        BlockTasksWritePlugin.reset();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ReindexPlugin.class,
            ReindexManagementPlugin.class,
            MainRestPlugin.class,
            TestTelemetryPlugin.class,
            BlockTasksWritePlugin.class
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*") // allow remote reindex
            .build();
    }

    public void testNonSlicedLocalReindexRelocation() throws Exception {
        final int slices = 1;
        testReindexRelocation(
            (nodeAName, nodeBName) -> startAsyncThrottledLocalReindexOnNode(nodeBName, slices),
            localReindexDescription(),
            slices,
            false,
            randomIntBetween(1, 4)
        );
    }

    public void testFixedSlicedLocalReindexRelocation() throws Exception {
        final int slices = randomIntBetween(2, 5);
        testReindexRelocation(
            (nodeAName, nodeBName) -> startAsyncThrottledLocalReindexOnNode(nodeBName, slices),
            localReindexDescription(),
            slices,
            false,
            randomIntBetween(1, 4)
        );
    }

    public void testAutoSlicedLocalReindexRelocation() throws Exception {
        final int slices = 0;
        testReindexRelocation(
            (nodeAName, nodeBName) -> startAsyncThrottledLocalReindexOnNode(nodeBName, slices),
            localReindexDescription(),
            slices,
            false,
            randomIntBetween(2, 4)
        );
    }

    public void testAutoNonSlicedLocalReindexRelocation() throws Exception {
        final int slices = 0;
        testReindexRelocation(
            (nodeAName, nodeBName) -> startAsyncThrottledLocalReindexOnNode(nodeBName, slices),
            localReindexDescription(),
            slices,
            false,
            1   // no slicing if only 1 shard
        );
    }

    public void testNonSlicedRemoteReindexRelocation() throws Exception {
        final int slices = 1;
        testReindexRelocation((nodeAName, nodeBName) -> {
            final InetSocketAddress nodeAAddress = internalCluster().getInstance(HttpServerTransport.class, nodeAName)
                .boundAddress()
                .publishAddress()
                .address();
            return startAsyncNonSlicedThrottledRemoteReindexOnNode(nodeBName, nodeAAddress);
        }, remoteReindexDescription(), slices, true, randomIntBetween(1, 4));
    }
    // no test for remote sliced reindex since it's not allowed

    private void testReindexRelocation(
        final CheckedBiFunction<String, String, TaskId, Exception> startReindexGivenNodeAAndB,
        final Matcher<String> expectedDescription,
        final int slices,
        final boolean isRemote,
        final int shards
    ) throws Exception {
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

        createIndexPinnedToNodeName(SOURCE_INDEX, nodeAName, shards);
        createIndexPinnedToNodeName(DEST_INDEX, nodeAName, shards);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        // Start throttled async reindex on nodeB and check it has the expected state
        final TaskId originalTaskId = startReindexGivenNodeAAndB.apply(nodeAName, nodeBName);
        assertBusy(() -> {
            final TaskResult originalReindex = getRunningReindex(originalTaskId);
            assertThat("reindex should start on nodeB", originalReindex.getTask().taskId().getNodeId(), equalTo(nodeBId));
            assertRunningReindexTaskExpectedState(originalReindex.getTask(), expectedDescription, slices, shards);
        });
        shutdownNodeNameAndRelocate(nodeBName);

        // Assert the original task is in .tasks index and has expected content (including relocated taskId on nodeA)
        final TaskId relocatedTaskId = assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(
            originalTaskId,
            nodeAId,
            expectedDescription,
            slices,
            shards
        );

        // Assert relocated reindex is running and has expected state
        assertBusy(() -> {
            final TaskResult relocatedReindex = getRunningReindex(relocatedTaskId);
            assertThat("relocated reindex should be on nodeA", relocatedReindex.getTask().taskId().getNodeId(), equalTo(nodeAId));
            assertRunningReindexTaskExpectedState(relocatedReindex.getTask(), expectedDescription, slices, shards);
            assertThat("relocated task should reference original", relocatedReindex.getTask().originalTaskId(), equalTo(originalTaskId));
        });

        // Speed up reindex post-relocation to keep the test fast
        unthrottleReindex(relocatedTaskId);

        assertRelocatedTaskExpectedEndState(relocatedTaskId, originalTaskId, expectedDescription, slices, shards);

        // Assert nodeA recorded success metrics for the relocated reindex
        assertReindexSuccessMetricsOnNode(nodeAName, isRemote, slices);

        // assert all documents have been reindexed
        assertExpectedNumberOfDocumentsInDestinationIndex();
    }

    /**
     * Forces the destination to write to .tasks first (by deferring the source's write), then releases the source.
     * The source's CREATE actually executes but is a no-op since the document already exists.
     */
    public void testTasksIndexDestinationWritesFirstThenSourceIsNoOp() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final int shards = randomIntBetween(1, 5);
        final var expectedDescription = localReindexDescription();

        final String destNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String destNodeId = nodeIdByName(destNodeName);
        final String sourceNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, destNodeName, shards);
        createIndexPinnedToNodeName(DEST_INDEX, destNodeName, shards);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledLocalReindexOnNode(sourceNodeName, 1);

        // Defer source writes so the destination writes first.
        // prepareForShutdown blocks until the task completes, but the source's deferred write keeps it waiting
        // use a background thread so the test can continue
        final CountDownLatch sourceWriteLatch = BlockTasksWritePlugin.deferWritesOnNode(sourceNodeName);
        final Thread shutdownThread = new Thread(() -> {
            try {
                internalCluster().getInstance(ShutdownPrepareService.class, sourceNodeName).prepareForShutdown();
            } catch (Exception e) {
                // expected — node may be stopped while waiting
            }
        });
        shutdownThread.setDaemon(true);
        shutdownThread.start();

        // Wait for the destination's write (not deferred) to create .tasks.
        ensureGreen(TaskResultsService.TASK_INDEX);
        assertThat("version is 1 after destination write", getTasksDocument(originalTaskId).getVersion(), is(1L));

        // Release the source's deferred CREATE
        sourceWriteLatch.countDown();

        final TaskId relocatedTaskId = assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(
            originalTaskId,
            destNodeId,
            expectedDescription,
            1,
            shards
        );
        unthrottleReindex(relocatedTaskId);
        assertRelocatedTaskExpectedEndState(relocatedTaskId, originalTaskId, expectedDescription, 1, shards);
        assertExpectedNumberOfDocumentsInDestinationIndex();

        assertThat("version stays 1 — source CREATE is a no-op", getTasksDocument(originalTaskId).getVersion(), is(1L));
    }

    /**
     * Forces the source to write first by deferring the destination's write. The destination's deferred INDEX then overwrites
     * the source's document, bumping the version to 2.
     */
    public void testTasksIndexSourceWritesFirstThenDestinationOverwrites() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final int shards = randomIntBetween(1, 5);
        final var expectedDescription = localReindexDescription();

        final String destNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String destNodeId = nodeIdByName(destNodeName);
        final String sourceNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, destNodeName, shards);
        createIndexPinnedToNodeName(DEST_INDEX, destNodeName, shards);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledLocalReindexOnNode(sourceNodeName, 1);

        // Defer the destination's write so the source writes first.
        final CountDownLatch destWriteLatch = BlockTasksWritePlugin.deferWritesOnNode(destNodeName);
        shutdownNodeNameAndRelocate(sourceNodeName);

        final GetResponse afterSourceWrite = getTasksDocument(originalTaskId);
        assertThat("version is 1 after source write", afterSourceWrite.getVersion(), is(1L));
        assertSourceAndDestinationStoreEquivalentResults(afterSourceWrite, BlockTasksWritePlugin.capturedDocumentSource(sourceNodeName));

        // Release the destination's deferred INDEX — it overwrites the source's document.
        destWriteLatch.countDown();
        assertBusy(() -> assertThat("version is 2 — destination overwrote source", getTasksDocument(originalTaskId).getVersion(), is(2L)));

        final TaskId relocatedTaskId = assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(
            originalTaskId,
            destNodeId,
            expectedDescription,
            1,
            shards
        );
        unthrottleReindex(relocatedTaskId);
        assertRelocatedTaskExpectedEndState(relocatedTaskId, originalTaskId, expectedDescription, 1, shards);
        assertExpectedNumberOfDocumentsInDestinationIndex();
    }

    /**
     * Verifies that the destination node writes the source task result to {@code .tasks} during relocation, so the chain link is preserved
     * even when the source node cannot write. The test uses {@link BlockTasksWritePlugin} to block all {@code .tasks} writes on the source
     * node, so only the destination's write (in {@code Reindexer.storeRelocationSourceTaskResult}) succeeds.
     */
    public void testTasksIndexDestinationWrites() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final int shards = randomIntBetween(1, 5);
        final var expectedDescription = localReindexDescription();

        final String destNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String destNodeId = nodeIdByName(destNodeName);
        final String sourceNodeName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, destNodeName, shards);
        createIndexPinnedToNodeName(DEST_INDEX, destNodeName, shards);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledLocalReindexOnNode(sourceNodeName, 1);
        final TaskResult running = getRunningReindex(originalTaskId);
        assertThat(running.getTask().taskId().getNodeId(), equalTo(nodeIdByName(sourceNodeName)));

        // Block .tasks writes on the source node so only the destination's write can succeed.
        BlockTasksWritePlugin.blockWritesOnNode(sourceNodeName);

        shutdownNodeNameAndRelocate(sourceNodeName);

        final TaskId relocatedTaskId = assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(
            originalTaskId,
            destNodeId,
            expectedDescription,
            1,
            shards
        );

        unthrottleReindex(relocatedTaskId);
        assertRelocatedTaskExpectedEndState(relocatedTaskId, originalTaskId, expectedDescription, 1, shards);
        assertExpectedNumberOfDocumentsInDestinationIndex();

        // Verify the document was written exactly once (by the destination) with correct content.
        final GetResponse doc = getTasksDocument(originalTaskId);
        assertThat("document should be written exactly once", doc.getVersion(), is(1L));
        assertTasksDocumentIsRelocatedException(doc, destNodeId);

        // Verify source and destination would store equivalent results.
        assertSourceAndDestinationStoreEquivalentResults(doc, BlockTasksWritePlugin.capturedDocumentSource(sourceNodeName));
    }

    /**
     * Test plugin that can block or defer {@code .tasks} index writes on specific nodes and captures the document body.
     * <p>
     * {@code blockedNodeName}: writes are rejected (failed) on this node.
     * {@code deferredNodeName} + {@code deferLatch}: writes are deferred on this node — the filter returns immediately (unblocking the
     * calling thread) and a background thread waits for the latch before proceeding. These two mechanisms are independent.
     */
    public static class BlockTasksWritePlugin extends Plugin implements ActionPlugin {
        private static volatile String blockedNodeName = null;
        private static volatile String deferredNodeName = null;
        private static volatile CountDownLatch deferLatch = null;
        private static final ConcurrentHashMap<String, BytesReference> capturedDocumentsByNode = new ConcurrentHashMap<>();
        private String myNodeName;

        /** Reject all {@code .tasks} writes on the given node. */
        public static void blockWritesOnNode(String nodeName) {
            blockedNodeName = nodeName;
        }

        /** Defer {@code .tasks} writes on the given node until the returned latch is released. */
        public static CountDownLatch deferWritesOnNode(String nodeName) {
            deferredNodeName = nodeName;
            deferLatch = new CountDownLatch(1);
            return deferLatch;
        }

        /** Returns the document source captured from the given node's intercepted write, or null. */
        public static BytesReference capturedDocumentSource(String nodeName) {
            return capturedDocumentsByNode.get(nodeName);
        }

        public static void reset() {
            blockedNodeName = null;
            deferredNodeName = null;
            deferLatch = null;
            capturedDocumentsByNode.clear();
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            myNodeName = Node.NODE_NAME_SETTING.get(services.environment().settings());
            assertNotNull(myNodeName);
            return List.of();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (isTasksIndexWrite(action, request)) {
                        captureDocumentSource((BulkRequest) request);
                        if (myNodeName.equals(blockedNodeName)) {
                            listener.onFailure(new ElasticsearchException("blocked .tasks write on [" + myNodeName + "] for testing"));
                            return;
                        }
                        final CountDownLatch latch = deferLatch;
                        if (latch != null && myNodeName.equals(deferredNodeName)) {
                            // fork to unblock the calling thread, also simulates async index write
                            final Thread deferThread = new Thread(() -> {
                                safeAwait(latch);
                                chain.proceed(task, action, request, listener);
                            }, "deferred-.tasks-write");
                            deferThread.setDaemon(true);
                            deferThread.start();
                            return;
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }

                private boolean isTasksIndexWrite(String action, ActionRequest request) {
                    if (action.equals(TransportBulkAction.NAME) && request instanceof BulkRequest bulkRequest) {
                        return bulkRequest.requests().stream().anyMatch(r -> TaskResultsService.TASK_INDEX.equals(r.index()));
                    }
                    return false;
                }

                private void captureDocumentSource(BulkRequest bulkRequest) {
                    for (DocWriteRequest<?> item : bulkRequest.requests()) {
                        if (TaskResultsService.TASK_INDEX.equals(item.index()) && item instanceof IndexRequest indexRequest) {
                            capturedDocumentsByNode.put(myNodeName, indexRequest.source());
                        }
                    }
                }
            });
        }
    }

    private GetResponse getTasksDocument(TaskId taskId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());
        final GetResponse response = client().prepareGet(TaskResultsService.TASK_INDEX, taskId.toString()).get();
        assertThat("task exists in .tasks index", response.isExists(), is(true));
        return response;
    }

    private static void assertTasksDocumentIsRelocatedException(GetResponse doc, String expectedDestNodeId) {
        final Map<String, Object> source = doc.getSourceAsMap();
        assertThat(source.get("completed"), is(true));
        @SuppressWarnings("unchecked")
        final Map<String, Object> error = (Map<String, Object>) source.get("error");
        assertThat(error.get("type"), equalTo("task_relocated_exception"));
        assertThat((String) error.get("relocated_task_id"), startsWith(expectedDestNodeId));
    }

    private static void assertSourceAndDestinationStoreEquivalentResults(GetResponse storedDoc, BytesReference capturedSource) {
        assertNotNull("source's write should have been captured", capturedSource);
        final Map<String, Object> storedByDestination = new HashMap<>(storedDoc.getSourceAsMap());
        final Map<String, Object> attemptedBySource = new HashMap<>(XContentHelper.convertToMap(capturedSource, false).v2());
        removeRunningTimeInNanos(storedByDestination);
        removeRunningTimeInNanos(attemptedBySource);
        assertThat("destination stores same result as source would have", storedByDestination, equalTo(attemptedBySource));
    }

    @SuppressWarnings("unchecked")
    private static void removeRunningTimeInNanos(Map<String, Object> taskResultMap) {
        ((Map<String, Object>) taskResultMap.get("task")).remove("running_time_in_nanos");
    }

    private void shutdownNodeNameAndRelocate(final String nodeName) throws Exception {
        // testing assumption: .tasks should not exist yet — it's created when the task result is stored during relocation
        assertFalse(".tasks index should not exist before shutdown", indexExists(TaskResultsService.TASK_INDEX));

        // trigger reindex relocation
        internalCluster().getInstance(ShutdownPrepareService.class, nodeName).prepareForShutdown();

        assertOnlyRelocationReindexMetricsOnNode(nodeName);

        // Wait for .tasks and replica to be created before stopping nodeB, otherwise the replica
        // on nodeA is stale and can't be promoted to primary when nodeB leaves
        assertBusy(() -> assertTrue(indexExists(TaskResultsService.TASK_INDEX)), 30, TimeUnit.SECONDS);
        ensureGreen(TaskResultsService.TASK_INDEX);

        internalCluster().stopNode(nodeName);
    }

    private TaskId assertOriginalTaskExpectedEndStateAndGetRelocatedTaskId(
        final TaskResult originalResult,
        final TaskId originalTaskId,
        final String relocatedNodeId,
        final Matcher<String> expectedTaskDescription,
        final int slices,
        final int shards
    ) {
        assertThat("task completed", originalResult.isCompleted(), is(true));

        final Map<String, Object> innerResponse = originalResult.getResponseAsMap();
        assertThat(innerResponse, equalTo(Map.of()));

        final TaskInfo taskInfo = originalResult.getTask();
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), is(expectedTaskDescription));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));

        final Map<String, Object> taskStatus = ((RawTaskStatus) taskInfo.status()).toMap();
        assertThat(taskStatus.get("slice_id"), is(nullValue()));
        assertThat((Integer) taskStatus.get("total"), lessThanOrEqualTo(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.get("updated"), is(0));
        assertThat((Integer) taskStatus.get("created"), lessThan(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.get("deleted"), is(0));
        assertThat(
            (Integer) taskStatus.get("batches"),
            lessThan((int) Math.floor((float) numberOfDocumentsThatTakes60SecondsToIngest / bulkSize))
        );
        assertThat(taskStatus.get("version_conflicts"), is(0));
        assertThat(taskStatus.get("noops"), is(0));
        assertThat(ObjectPath.eval("retries.bulk", taskStatus), is(0));
        assertThat(ObjectPath.eval("retries.search", taskStatus), is(0));
        assertThat((Integer) taskStatus.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat((double) taskStatus.get("requests_per_second"), closeTo(requestsPerSecond, 0.00001));

        assertThat(taskStatus.get("reason_cancelled"), is(nullValue()));
        assertThat((Integer) taskStatus.get("throttled_until_millis"), greaterThanOrEqualTo(0));

        if (isSliced(slices, shards)) {
            final int expectedSlices = getExpectedSlices(slices, shards);
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> sliceStatuses = (List<Map<String, Object>>) taskStatus.get("slices");
            assertThat(sliceStatuses.size(), equalTo(expectedSlices));
            for (int i = 0; i < expectedSlices; i++) {
                final Map<String, Object> slice = sliceStatuses.get(i);
                assertThat(slice.get("slice_id"), is(i));
                assertThat((double) slice.get("requests_per_second"), closeTo((double) requestsPerSecond / expectedSlices, 0.00001));
            }
        } else {
            assertThat(taskStatus.containsKey("slices"), is(false));
        }

        final Map<String, Object> errorMap = originalResult.getErrorAsMap();
        assertThat(errorMap, is(aMapWithSize(4)));
        assertThat("we get expected error type", errorMap.get("type"), equalTo("task_relocated_exception"));
        assertThat("we get expected error reason", errorMap.get("reason"), equalTo("Task was relocated"));
        assertThat("we get expected original task id", errorMap.get("original_task_id"), equalTo(originalTaskId.toString()));
        final String relocatedTaskId = (String) errorMap.get("relocated_task_id");
        assertThat("we relocate to expected node", relocatedTaskId, startsWith(relocatedNodeId));
        return new TaskId(relocatedTaskId);
    }

    private void assertRelocatedTaskExpectedEndState(
        final TaskId taskId,
        final TaskId originalTaskId,
        final Matcher<String> expectedTaskDescription,
        final int slices,
        final int shards
    ) throws Exception {
        final SetOnce<TaskResult> finishedResult = new SetOnce<>();

        assertBusy(() -> finishedResult.set(getCompletedTaskResult(taskId)), 30, TimeUnit.SECONDS);
        final TaskResult result = finishedResult.get();
        assertThat("relocated task has no error", result.getError(), is(nullValue()));
        final Map<String, Object> innerResponse = result.getResponseAsMap();
        assertThat(innerResponse.get("timed_out"), is(false));
        assertThat(innerResponse.get("total"), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(innerResponse.get("updated"), is(0));
        assertThat(innerResponse.get("created"), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(innerResponse.get("deleted"), is(0));
        assertThat(
            (int) innerResponse.get("batches"),
            greaterThanOrEqualTo((int) Math.ceil((float) numberOfDocumentsThatTakes60SecondsToIngest / bulkSize))
        );
        assertThat(innerResponse.get("version_conflicts"), is(0));
        assertThat(innerResponse.get("noops"), is(0));
        assertThat((Integer) innerResponse.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat(innerResponse.get("requests_per_second"), is(-1.0));
        assertThat((Integer) innerResponse.get("throttled_until_millis"), greaterThanOrEqualTo(0));
        assertThat(innerResponse.get("failures"), is(List.of()));

        final TaskInfo taskInfo = result.getTask();
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), is(expectedTaskDescription));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));
        assertThat("completed relocated task should reference original", taskInfo.originalTaskId(), equalTo(originalTaskId));

        final Map<String, Object> taskStatus = ((RawTaskStatus) taskInfo.status()).toMap();
        assertThat(taskStatus.get("slice_id"), is(nullValue()));
        assertThat(taskStatus.get("total"), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.get("updated"), is(0));
        assertThat(taskStatus.get("created"), is(numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.get("deleted"), is(0));
        assertThat(
            (int) taskStatus.get("batches"),
            greaterThanOrEqualTo((int) Math.ceil((float) numberOfDocumentsThatTakes60SecondsToIngest / bulkSize))
        );
        assertThat(taskStatus.get("version_conflicts"), is(0));
        assertThat(taskStatus.get("noops"), is(0));
        assertThat(ObjectPath.eval("retries.bulk", taskStatus), is(0));
        assertThat(ObjectPath.eval("retries.search", taskStatus), is(0));
        assertThat((Integer) taskStatus.get("throttled_millis"), greaterThanOrEqualTo(0));
        assertThat(taskStatus.get("requests_per_second"), is(-1.0));
        assertThat(taskStatus.get("reason_cancelled"), is(nullValue()));
        assertThat((Integer) taskStatus.get("throttled_until_millis"), greaterThanOrEqualTo(0));

        if (isSliced(slices, shards)) {
            final int expectedSlices = getExpectedSlices(slices, shards);
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> responseSlices = (List<Map<String, Object>>) innerResponse.get("slices");
            assertThat(responseSlices.size(), equalTo(expectedSlices));
            int totalCreated = 0;
            for (Map<String, Object> slice : responseSlices) {
                assertThat(slice.get("requests_per_second"), is(-1.0));
                totalCreated += (Integer) slice.get("created");
            }
            assertThat(totalCreated, equalTo(numberOfDocumentsThatTakes60SecondsToIngest));
        } else {
            assertThat(innerResponse.containsKey("slices"), is(false));
        }
    }

    private TaskId assertOriginalTaskEndStateInTasksIndexAndGetRelocatedTaskId(
        final TaskId taskId,
        final String relocatedNodeId,
        final Matcher<String> expectedTaskDescription,
        final int slices,
        final int shards
    ) {
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

        return assertOriginalTaskExpectedEndStateAndGetRelocatedTaskId(
            result,
            taskId,
            relocatedNodeId,
            expectedTaskDescription,
            slices,
            shards
        );
    }

    private TaskId startAsyncThrottledLocalReindexOnNode(final String nodeName, final int slices) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", slices == 0 ? "auto" : Integer.toString(slices));
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

    private TaskId startAsyncNonSlicedThrottledRemoteReindexOnNode(final String nodeName, final InetSocketAddress remoteAddress)
        throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", Integer.toString(1));
            request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
            request.setJsonEntity(Strings.format("""
                {
                  "source": {
                    "remote": {
                      "host": "http://%s:%d"
                    },
                    "index": "%s",
                    "size": %d
                  },
                  "dest": {
                    "index": "%s"
                  }
                }
                """, InetAddresses.toUriString(remoteAddress.getAddress()), remoteAddress.getPort(), SOURCE_INDEX, bulkSize, DEST_INDEX));

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private static Matcher<String> localReindexDescription() {
        return equalTo(Strings.format("reindex from [%s] to [%s]", SOURCE_INDEX, DEST_INDEX));
    }

    private static Matcher<String> remoteReindexDescription() {
        return allOf(startsWith("reindex from [host="), endsWith(Strings.format("[%s] to [%s]", SOURCE_INDEX, DEST_INDEX)));
    }

    private TaskResult getRunningReindex(final TaskId taskId) {
        final TaskResult reindex = clusterAdmin().prepareGetTask(taskId).get().getTask();
        assertThat("reindex is running", reindex.isCompleted(), is(false));
        return reindex;
    }

    private void assertRunningReindexTaskExpectedState(
        final TaskInfo taskInfo,
        final Matcher<String> expectedTaskDescription,
        final int slices,
        final int shards
    ) {
        assertThat(taskInfo.action(), equalTo(ReindexAction.NAME));
        assertThat(taskInfo.description(), is(expectedTaskDescription));
        assertThat(taskInfo.cancelled(), equalTo(false));
        assertThat(taskInfo.cancellable(), equalTo(true));

        final BulkByScrollTask.Status taskStatus = ((BulkByScrollTask.Status) taskInfo.status());
        // lessThan because the initial running reindex might have "uninitialized" 0
        assertThat(taskStatus.getTotal(), lessThanOrEqualTo((long) numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.getUpdated(), is(0L));
        assertThat(taskStatus.getCreated(), lessThan((long) numberOfDocumentsThatTakes60SecondsToIngest));
        assertThat(taskStatus.getDeleted(), is(0L));
        assertThat(taskStatus.getBatches(), lessThan((int) Math.ceil((float) numberOfDocumentsThatTakes60SecondsToIngest / bulkSize)));
        assertThat(taskStatus.getVersionConflicts(), is(0L));
        assertThat(taskStatus.getNoops(), is(0L));
        assertThat(taskStatus.getBulkRetries(), is(0L));
        assertThat(taskStatus.getSearchRetries(), is(0L));
        assertThat(taskStatus.getThrottled(), greaterThanOrEqualTo(TimeValue.ZERO));
        assertThat(taskStatus.getRequestsPerSecond(), equalTo((float) requestsPerSecond));
        assertThat(taskStatus.getReasonCancelled(), is(nullValue()));
        assertThat(taskStatus.getThrottledUntil(), greaterThanOrEqualTo(TimeValue.ZERO));

        if (isSliced(slices, shards)) {
            final int expectedSlices = getExpectedSlices(slices, shards);
            final List<BulkByScrollTask.StatusOrException> expectedStatuses = Collections.nCopies(expectedSlices, null);
            assertThat("running slices statuses are null", taskStatus.getSliceStatuses(), equalTo(expectedStatuses));
        } else {
            assertThat(taskStatus.getSliceStatuses().isEmpty(), is(true));
        }
    }

    private boolean isSliced(int slices, int shards) {
        return slices > 1 || (slices == 0 && shards > 1);
    }

    private int getExpectedSlices(int slices, int shards) {
        if (slices > 1) {
            return slices;
        } else if (slices == 0) {
            return Math.max(shards, 1);
        } else {
            return 1;
        }
    }

    private TaskResult getCompletedTaskResult(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(true));
        return task;
    }

    private void createIndexPinnedToNodeName(final String index, final String nodeName, final int shards) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), index);
    }

    private void unthrottleReindex(final TaskId taskId) {
        try {
            final RestClient restClient = getRestClient();
            final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
            request.addParameter("requests_per_second", Integer.toString(-1));
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

    private TestTelemetryPlugin getTelemetryPlugin(final String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }

    private void assertOnlyRelocationReindexMetricsOnNode(final String nodeName) {
        final TestTelemetryPlugin plugin = getTelemetryPlugin(nodeName);
        plugin.collect();
        assertThat(plugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_COMPLETION_COUNTER), is(empty()));
        assertThat(plugin.getLongHistogramMeasurement(ReindexMetrics.REINDEX_TIME_HISTOGRAM), is(empty()));
        final var relocationCounter = plugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_RELOCATION_COUNTER);
        assertThat("relocation metric updated", relocationCounter.size(), equalTo(1));
        assertThat("relocation metric updated", relocationCounter.getFirst().getLong(), equalTo(1L));
        assertThat(
            "relocation metric was successful",
            relocationCounter.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE),
            is(nullValue())
        );
    }

    private void assertReindexSuccessMetricsOnNode(final String nodeName, final boolean isRemote, final int slices) {
        final TestTelemetryPlugin plugin = getTelemetryPlugin(nodeName);
        plugin.collect();
        final List<Measurement> completions = plugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_COMPLETION_COUNTER);
        assertThat(completions.size(), equalTo(1));
        assertNull(completions.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE));
        final String expectedSource = isRemote ? ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_REMOTE : ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL;
        assertThat(completions.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SOURCE), equalTo(expectedSource));
        SlicingMode slicingMode = null;
        if (slices == 0) {
            slicingMode = SlicingMode.AUTO;
        } else if (slices == 1) {
            slicingMode = SlicingMode.NONE;
        } else if (slices > 1) {
            slicingMode = SlicingMode.FIXED;
        } else {
            fail("invalid slices value: " + slices);
        }
        assertThat(
            completions.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE),
            equalTo(slicingMode.name().toLowerCase(Locale.ROOT))
        );
        final List<Measurement> durations = plugin.getLongHistogramMeasurement(ReindexMetrics.REINDEX_TIME_HISTOGRAM);
        assertThat(durations.size(), equalTo(1));
        final Measurement duration = durations.getFirst();
        assertThat(duration.getLong(), greaterThanOrEqualTo(0L));
        assertThat(duration.attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SOURCE), equalTo(expectedSource));
        assertThat(
            duration.attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE),
            equalTo(slicingMode.name().toLowerCase(Locale.ROOT))
        );
        assertThat("no relocation metric", plugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_RELOCATION_COUNTER).size(), equalTo(0));
    }

    private void assertExpectedNumberOfDocumentsInDestinationIndex() throws IOException {
        assertNoFailures(indicesAdmin().prepareRefresh(DEST_INDEX).get());
        final Request request = new Request("GET", "/" + DEST_INDEX + "/_count");
        final Response response = getRestClient().performRequest(request);
        final Map<?, ?> body = ESRestTestCase.entityAsMap(response);
        final int count = ((Number) body.get("count")).intValue();
        assertThat(count, equalTo(numberOfDocumentsThatTakes60SecondsToIngest));
    }
}
