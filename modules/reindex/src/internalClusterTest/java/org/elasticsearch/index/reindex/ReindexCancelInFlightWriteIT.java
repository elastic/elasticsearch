/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.management.GetReindexRequest;
import org.elasticsearch.reindex.management.ReindexManagementPlugin;
import org.elasticsearch.reindex.management.TransportGetReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/// Reproduces the use-after-free that a reindex cancellation can cause on the destination write path.
///
/// `BulkOperation` dispatches one shard level request per destination shard. If dispatching a later shard throws synchronously,
/// which is what [TaskManager#registerChildConnection] does once the parent task has banned its children, the throw escapes the
/// dispatch loop and completes the top-level bulk listener while earlier shard requests are still writing. Reindex releases the
/// pooled search hit bytes when that listener completes, so the in-flight write is left reading freed memory.
///
/// Four things have to be true at once for that shared memory to exist, and for the ban to have a later dispatch to reject:
///
///  - The destination bulk covers two or more shards, so the dispatch loop still has a shard left when the ban lands. With one
///    destination shard there is no second dispatch and nothing throws.
///  - The destination primary is on the coordinating node. `TransportService#sendLocalRequest` hands the request object straight
///    to the handler, so the `IndexRequest` reaches the write thread still pointing at the search hit bytes. A remote primary
///    serializes the request instead, and the write then reads a copy owned by the inbound transport buffer, which the reindex
///    does not free.
///  - The source search runs its fetch as a separate phase, which needs two or more source shards. When
///    `SearchService#executeQueryPhase` sees `request.numberOfShards() == 1` it runs the fetch inline against the same context,
///    so the search never reaches `SearchTransportService#sendExecuteFetch` and never produces pooled hits.
///  - That separate fetch phase takes the chunked path, which serializes hits into a pooled buffer on the data node and gives
///    the coordinator retained [ReleasableBytesReference] slices of it rather than copies. `SearchTransportService#sendExecuteFetch`
///    excludes cross-cluster search, scroll, and data nodes older than `CHUNKED_FETCH_DOC_ID_ORDER`, and is gated on
///    `search.fetch_phase_chunked_enabled`. Reindex paginates with a point-in-time reader rather than a scroll, so it is not
///    excluded.
///
/// One node with two shards per index satisfies all four requirements.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ReindexCancelInFlightWriteIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_cancel_src";
    private static final String DESTINATION_INDEX = "reindex_cancel_dst";

    /// More than one source shard so the fetch runs as a separate chunked phase and the hits arrive as pooled slices.
    private static final int SOURCE_SHARDS = 2;
    /// More than one destination shard so the dispatch loop has a second shard left to reject once the ban lands.
    private static final int DESTINATION_SHARDS = 2;
    private static final int DOC_COUNT = 200;
    private static final String BAN_REASON = "by user request";
    private static final String CANCELLATION_MESSAGE = "parent task was cancelled [" + BAN_REASON + "]";
    /// Reindex shouldn't stop within this window. If it has, it has not waited on the write to finish, which leads to a leak.
    private static final TimeValue EARLY_COMPLETION_WATCH_WINDOW = TimeValue.timeValueSeconds(5);
    /// How long to wait for write to be released before failing the test, to not hang the suite.
    private static final TimeValue PARKED_WRITE_TIMEOUT = TimeValue.timeValueSeconds(15);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, ReindexManagementPlugin.class, CancelOrchestrationPlugin.class);
    }

    @Before
    public void resetOrchestration() {
        CancelOrchestrationPlugin.reset();
    }

    public void testDestinationWriteOutlivesCancelledBulk() throws Exception {
        createTestIndex(SOURCE_INDEX, SOURCE_SHARDS);
        createTestIndex(DESTINATION_INDEX, DESTINATION_SHARDS);

        indexRandom(true, SOURCE_INDEX, DOC_COUNT);

        final ReindexRequestBuilder reindex = new ReindexRequestBuilder(client()).source(SOURCE_INDEX)
            .destination(DESTINATION_INDEX)
            .setShouldStoreResult(true);
        // A single batch covering every document, so it necessarily spans both destination shards
        reindex.source().setSize(DOC_COUNT);

        final ActionFuture<BulkByPaginatedSearchResponse> reindexFuture = client().execute(ReindexAction.INSTANCE, reindex.request());

        final boolean reindexFinishedWhileWriteParked;
        try {
            safeAwait(CancelOrchestrationPlugin.destinationWriteParked);
            reindexFinishedWhileWriteParked = waitUntil(reindexFuture::isDone, EARLY_COMPLETION_WATCH_WINDOW.seconds(), TimeUnit.SECONDS);
        } finally {
            CancelOrchestrationPlugin.releaseDestinationWrite.countDown();
        }
        safeAwait(CancelOrchestrationPlugin.destinationWriteReleased);

        logger.info(
            "destination bulk had [{}] items, its first shard request had [{}], the ban saw [{}] pending child connections",
            CancelOrchestrationPlugin.destinationBulkItemCount.get(),
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            CancelOrchestrationPlugin.pendingChildConnectionsAtBan.get()
        );

        final String premiseFailureMessage = CancelOrchestrationPlugin.premiseFailure.get();
        if (premiseFailureMessage != null) {
            fail("a reproduction premise broke, so nothing was proven: " + premiseFailureMessage);
        }
        assertThat(
            "every document has to land in one bulk, otherwise the item counts below are comparing across batches",
            CancelOrchestrationPlugin.destinationBulkItemCount.get(),
            equalTo(DOC_COUNT)
        );
        assertThat(
            "the first destination shard request has to cover only part of the bulk, otherwise the dispatch loop has no second shard "
                + "left for the ban to reject",
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            lessThan(CancelOrchestrationPlugin.destinationBulkItemCount.get())
        );
        assertThat(
            "the ban has to find the bulk task with the first shard request still registered as a child, otherwise it is a no-op and "
                + "no later dispatch is rejected",
            CancelOrchestrationPlugin.pendingChildConnectionsAtBan.get(),
            greaterThan(0)
        );

        assertFalse(
            "destination write resumed after the reindexed document source had been released",
            CancelOrchestrationPlugin.sourceReleasedUnderWrite.get()
        );
        assertFalse("reindex completed while a destination write it had dispatched was still in flight", reindexFinishedWhileWriteParked);

        final Throwable reindexFailure = ExceptionsHelper.unwrapCause(
            expectThrows(Exception.class, () -> reindexFuture.actionGet(SAFE_AWAIT_TIMEOUT))
        );
        assertThat(reindexFailure, instanceOf(TaskCancelledException.class));
        assertThat(reindexFailure.getMessage(), equalTo(CANCELLATION_MESSAGE));

        assertStoredTaskOutcome(CancelOrchestrationPlugin.reindexTaskId.get());

        // check the first shard bulk landed
        indicesAdmin().prepareRefresh(DESTINATION_INDEX).get();
        assertHitCount(
            prepareSearch(DESTINATION_INDEX).setSize(0).setTrackTotalHits(true),
            CancelOrchestrationPlugin.firstShardRequestItemCount.get()
        );
    }

    /// Asserts the terminal task document behind `GET _tasks/{id}` and `GET _reindex/{id}`, which is all a caller has left once the reindex
    /// call itself has failed. Both APIs read the same stored `TaskResult`, and `_reindex` only re-renders the task header through its own
    /// allowlist, so their outcome halves are expected to be identical.
    private void assertStoredTaskOutcome(TaskId reindexTaskId) {
        assertTrue("the reindex task id was never captured, so there is nothing to look up", reindexTaskId.isSet());

        final GetReindexRequest getReindex = new GetReindexRequest(reindexTaskId, false, null);
        final Map<String, Object> tasksBody = renderAndLog("_tasks/" + reindexTaskId, clusterAdmin().prepareGetTask(reindexTaskId).get());
        final Map<String, Object> reindexBody = renderAndLog(
            "_reindex/" + reindexTaskId,
            client().execute(TransportGetReindexAction.TYPE, getReindex).actionGet(SAFE_AWAIT_TIMEOUT)
        );

        final Map<String, Object> cancellation = Map.of("type", "task_cancelled_exception", "reason", CANCELLATION_MESSAGE);
        assertThat(tasksBody.get("completed"), equalTo(true));
        assertThat(tasksBody.get("error"), equalTo(cancellation));
        assertThat(tasksBody, not(hasKey("response")));

        assertThat(
            reindexBody.keySet(),
            containsInAnyOrder(
                "completed",
                "id",
                "description",
                "start_time_in_millis",
                "running_time_in_nanos",
                "cancelled",
                "status",
                "error"
            )
        );
        assertThat(reindexBody, not(hasKey("response")));
        assertThat(reindexBody.get("completed"), equalTo(true));
        assertThat(reindexBody.get("id"), equalTo(reindexTaskId.toString()));
        assertThat(reindexBody.get("description"), equalTo("reindex from [" + SOURCE_INDEX + "] to [" + DESTINATION_INDEX + "]"));
        // .tasks system index doesn't store this, so it's confusingly false
        assertThat(reindexBody.get("cancelled"), equalTo(false));
        assertThat("the two APIs must not disagree about the outcome", reindexBody.get("error"), equalTo(tasksBody.get("error")));

        // The status counters show what the reindex believes it did. Nothing is counted as created because onBulkResponse never runs on
        // the failure path, even though the shard request that was already in flight wrote every one of its documents.
        // todo(szybia): create a bug
        final Map<String, Object> status = ObjectPath.eval("status", reindexBody);
        assertThat(status.get("total"), equalTo(DOC_COUNT));
        assertThat(status.get("batches"), equalTo(1));
        assertThat(status.get("created"), equalTo(0));
        assertThat(status.get("canceled"), equalTo(CANCELLATION_MESSAGE));
    }

    /// Renders a management API response the way the REST layer does and returns it as a map, logging the body so the whole outcome shows
    /// up in the test output rather than only the fields asserted below.
    private Map<String, Object> renderAndLog(String api, ToXContentObject response) {
        final String body = Strings.toString(response, true, false);
        logger.info("GET {} returned:\n{}", api, body);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), body, false);
    }

    private void createTestIndex(String index, int numberOfShards) {
        prepareCreate(index).setSettings(indexSettings(numberOfShards, 0)).get();
        ensureGreen(index);
    }

    /// Interleaves the two events the reproduction needs: a destination primary write parked while it still points at the reindexed
    /// document's pooled source bytes, and a child task ban landing between two shard dispatches of the same bulk.
    public static class CancelOrchestrationPlugin extends Plugin implements ActionPlugin {

        static volatile CountDownLatch destinationWriteParked;
        static volatile CountDownLatch releaseDestinationWrite;
        static volatile CountDownLatch destinationWriteReleased;

        /// Assigned once per node start, which happens before [#reset] runs, so no need for it in reset.
        static volatile TaskManager coordinatorTaskManager;

        static final AtomicBoolean firstShardDispatchSeen = new AtomicBoolean();
        static final AtomicBoolean firstWriteSeen = new AtomicBoolean();
        static final AtomicBoolean sourceReleasedUnderWrite = new AtomicBoolean();
        static final AtomicReference<String> premiseFailure = new AtomicReference<>();
        static final AtomicInteger destinationBulkItemCount = new AtomicInteger();
        static final AtomicInteger firstShardRequestItemCount = new AtomicInteger();
        static final AtomicInteger pendingChildConnectionsAtBan = new AtomicInteger(-1);
        /// Captured while the reindex is running, because once it has finished the only way to reach its outcome is by task id and the
        /// client call does not hand one back.
        static final AtomicReference<TaskId> reindexTaskId = new AtomicReference<>(TaskId.EMPTY_TASK_ID);

        static void reset() {
            destinationWriteParked = new CountDownLatch(1);
            releaseDestinationWrite = new CountDownLatch(1);
            destinationWriteReleased = new CountDownLatch(1);
            firstShardDispatchSeen.set(false);
            firstWriteSeen.set(false);
            sourceReleasedUnderWrite.set(false);
            premiseFailure.set(null);
            destinationBulkItemCount.set(0);
            firstShardRequestItemCount.set(0);
            pendingChildConnectionsAtBan.set(-1);
            reindexTaskId.set(TaskId.EMPTY_TASK_ID);
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            coordinatorTaskManager = services.taskManager();
            return List.of();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (DESTINATION_INDEX.equals(indexModule.getIndex().getName())) {
                indexModule.addIndexOperationListener(new ParkFirstDestinationWrite());
            }
        }

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new RecordDestinationBulkSize(), new BanChildrenBetweenShardDispatches());
        }

        static void recordPremiseFailure(String message) {
            premiseFailure.compareAndSet(null, message);
        }

        /// Neither hook may use `safeAwait`. It fails with an [AssertionError], which is not caught by the runnables driving the write
        /// path or by the action filter chain, so a timeout would abandon a shard permit and hang the whole suite instead of reporting
        /// the premise that broke.
        static boolean awaitQuietly(CountDownLatch latch, TimeValue timeout) {
            try {
                return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /// Records how many items the reindex put in one bulk, which is what tells us whether a single shard request could have covered
        /// the whole batch, and the id of the reindex task that issued it.
        private static class RecordDestinationBulkSize implements MappedActionFilter {

            @Override
            public String actionName() {
                return TransportBulkAction.NAME;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (request instanceof BulkRequest bulkRequest
                    && bulkRequest.requests().isEmpty() == false
                    && DESTINATION_INDEX.equals(bulkRequest.requests().get(0).index())) {
                    destinationBulkItemCount.compareAndSet(0, bulkRequest.numberOfActions());
                    // Reindex defaults to one slice, so the bulk's parent is the unsliced task the client is waiting on rather than a
                    // slice worker, which is what GET _reindex/{id} requires
                    reindexTaskId.compareAndSet(TaskId.EMPTY_TASK_ID, task.getParentTaskId());
                }
                chain.proceed(task, action, request, listener);
            }
        }

        /// Holds the first destination primary write open while the rest of the bulk is dispatched, then records whether the document
        /// source it is about to index was freed underneath it.
        private static class ParkFirstDestinationWrite implements IndexingOperationListener {

            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
                if (index.origin() != Engine.Operation.Origin.PRIMARY || firstWriteSeen.compareAndSet(false, true) == false) {
                    return index;
                }
                final BytesReference source = index.parsedDoc().source().originalBytes();
                try {
                    destinationWriteParked.countDown();
                    if (awaitQuietly(releaseDestinationWrite, PARKED_WRITE_TIMEOUT) == false) {
                        recordPremiseFailure("the parked destination write was never released");
                    } else if (source instanceof ReleasableBytesReference pooled) {
                        // hasReferences() is the only accessor that stays safe to call once the bytes have been released
                        sourceReleasedUnderWrite.set(!pooled.hasReferences());
                    } else {
                        recordPremiseFailure(
                            "the reindexed document source was not pooled, so it cannot be freed under the write: "
                                + source.getClass().getSimpleName()
                        );
                    }
                } finally {
                    destinationWriteReleased.countDown();
                }
                return index;
            }
        }

        /// Runs inline on the thread driving `BulkOperation`'s dispatch loop, which makes it the only hook available between two shard
        /// dispatches of the same bulk. Once the first destination shard is writing, it bans the bulk task's children so the next
        /// dispatch throws out of [TaskManager#registerChildConnection] exactly as it would under a real cancellation. Banning
        /// directly rather than issuing a cancel keeps the interleaving deterministic.
        private static class BanChildrenBetweenShardDispatches implements MappedActionFilter {

            @Override
            public String actionName() {
                return TransportShardBulkAction.ACTION_NAME;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (request instanceof BulkShardRequest shardRequest
                    && DESTINATION_INDEX.equals(shardRequest.index())
                    && firstShardDispatchSeen.compareAndSet(false, true)) {
                    firstShardRequestItemCount.set(shardRequest.items().length);
                    final TaskId parentTaskId = shardRequest.getParentTask();
                    // TransportShardBulkAction registers its primary handler with the write pool, and
                    // TransportReplicationAction#handlePrimaryRequest dispatches to it before taking a permit, so this call returns
                    // once the operation is queued rather than once it is written. That is what frees this thread to ban the parent
                    // while the first shard is still writing.
                    chain.proceed(task, action, request, listener);
                    if (awaitQuietly(destinationWriteParked, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the first destination shard request never reached the engine");
                    } else if (parentTaskId.isSet() == false) {
                        recordPremiseFailure("the destination shard request carried no parent bulk task to ban");
                    } else {
                        pendingChildConnectionsAtBan.set(
                            coordinatorTaskManager.startBanOnChildTasks(parentTaskId.getId(), BAN_REASON, () -> {}).size()
                        );
                    }
                    return;
                }
                chain.proceed(task, action, request, listener);
            }
        }
    }
}
