/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.pooled;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/// A bulk request may alias pooled bytes that its caller owns, and a caller is entitled to let go of those bytes once the bulk
/// listener has completed. Cancellation makes the two collide: dispatching a later shard throws synchronously out of
/// [TaskManager#registerChildConnection] once the bulk task's children are banned, which fails the bulk listener while an earlier
/// shard request is still writing.
///
/// This test plays the caller, so it owns the pooled source bytes outright and can watch exactly when they are freed. Two shards on
/// a single node are what the scenario needs: two so the dispatch loop still has a shard left to reject once the ban lands, and one
/// node so that `TransportService#sendLocalRequest` hands the request object to the write thread by reference rather than a
/// serialized copy of it.
///
/// `ReindexCancelInFlightWriteIT`, in the reindex module, covers the same contract with a real caller whose bytes come from a
/// search fetch.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class BulkCancelInFlightWriteIT extends ESIntegTestCase {

    private static final String INDEX = "bulk_cancel_idx";
    /// More than one shard so the dispatch loop has a second shard left to reject once the ban lands.
    private static final int SHARDS = 2;
    private static final int DOC_COUNT = 100;
    private static final String BAN_REASON = "by user request";
    private static final String CANCELLATION_MESSAGE = "parent task was cancelled [" + BAN_REASON + "]";
    /// How long to wait for write to be released before failing the test, to not hang the suite.
    private static final TimeValue PARKED_WRITE_TIMEOUT = TimeValue.timeValueSeconds(15);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CancelOrchestrationPlugin.class);
    }

    @Before
    public void resetOrchestration() {
        CancelOrchestrationPlugin.reset();
    }

    public void testWriteOutlivesCancelledBulk() throws Exception {
        // An explicit mapping keeps a dynamic mapping update from having to reach the master while the bulk task's children are banned
        prepareCreate(INDEX).setSettings(indexSettings(SHARDS, 0)).setMapping("field", "type=keyword").get();
        ensureGreen(INDEX);

        final AtomicInteger releases = new AtomicInteger();
        final List<ReleasableBytesReference> sources = new ArrayList<>(DOC_COUNT);
        final BulkRequest bulkRequest = new BulkRequest();
        // One update item so the retention covers requests that carry their source somewhere other than an IndexRequest of their own
        final int updateSlot = randomIntBetween(0, DOC_COUNT - 1);
        for (int i = 0; i < DOC_COUNT; i++) {
            ReleasableBytesReference source = pooled(new BytesArray("{\"field\":\"" + randomAlphaOfLength(20) + "\"}"), releases);
            sources.add(source);
            if (i == updateSlot) {
                bulkRequest.add(new UpdateRequest(INDEX, Integer.toString(i)).doc(source, XContentType.JSON).docAsUpsert(true));
            } else {
                bulkRequest.add(new IndexRequest(INDEX).id(Integer.toString(i)).source(source, XContentType.JSON));
            }
        }

        final PlainActionFuture<BulkResponse> bulkFuture = new PlainActionFuture<>();
        client().execute(TransportBulkAction.TYPE, bulkRequest, ActionListener.releaseBefore(Releasables.wrap(sources), bulkFuture));

        safeAwait(CancelOrchestrationPlugin.writeParked);

        // A cancelled bulk fails fast rather than waiting for the shard requests it has already dispatched, so this returns while the
        // write is still parked, and the caller's references are gone by the time it does
        final Throwable failure;
        try {
            failure = ExceptionsHelper.unwrapCause(expectThrows(Exception.class, () -> bulkFuture.actionGet(SAFE_AWAIT_TIMEOUT)));
        } finally {
            CancelOrchestrationPlugin.releaseWrite.countDown();
        }
        safeAwait(CancelOrchestrationPlugin.writeReleased);

        logger.info(
            "bulk had [{}] items, its first shard request had [{}], the ban saw [{}] pending child connections",
            DOC_COUNT,
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            CancelOrchestrationPlugin.pendingChildConnectionsAtBan.get()
        );

        final String premiseFailureMessage = CancelOrchestrationPlugin.premiseFailure.get();
        if (premiseFailureMessage != null) {
            fail("a reproduction premise broke, so nothing was proven: " + premiseFailureMessage);
        }
        assertThat(
            "the first shard request has to cover only part of the bulk, otherwise the dispatch loop has no second shard left for the "
                + "ban to reject",
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            lessThan(DOC_COUNT)
        );
        assertThat(
            "the ban has to find the bulk task with the first shard request still registered as a child, otherwise it is a no-op and "
                + "no later dispatch is rejected",
            CancelOrchestrationPlugin.pendingChildConnectionsAtBan.get(),
            greaterThan(0)
        );

        assertFalse(
            "write resumed after the source of the document it was indexing had been released",
            CancelOrchestrationPlugin.sourceReleasedUnderWrite.get()
        );

        assertThat(failure, instanceOf(TaskCancelledException.class));
        assertThat(failure.getMessage(), equalTo(CANCELLATION_MESSAGE));

        // Every retention is released once its shard is done with it, so nothing is held past the last in flight write
        assertBusy(() -> assertThat(releases.get(), equalTo(DOC_COUNT)));
        sources.forEach(source -> assertFalse(source.hasReferences()));

        indicesAdmin().prepareRefresh(INDEX).get();
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), CancelOrchestrationPlugin.firstShardRequestItemCount.get());
    }

    /// Interleaves the two events the reproduction needs: a primary write parked while it still points at the caller's pooled source
    /// bytes, and a child task ban landing between two shard dispatches of the same bulk.
    public static class CancelOrchestrationPlugin extends Plugin implements ActionPlugin {

        static volatile CountDownLatch writeParked;
        static volatile CountDownLatch releaseWrite;
        static volatile CountDownLatch writeReleased;

        /// Assigned once per node start, which happens before [#reset] runs, so no need for it in reset.
        static volatile TaskManager coordinatorTaskManager;

        static final AtomicBoolean firstShardDispatchSeen = new AtomicBoolean();
        static final AtomicBoolean firstWriteSeen = new AtomicBoolean();
        static final AtomicBoolean sourceReleasedUnderWrite = new AtomicBoolean();
        static final AtomicReference<String> premiseFailure = new AtomicReference<>();
        static final AtomicInteger firstShardRequestItemCount = new AtomicInteger();
        static final AtomicInteger pendingChildConnectionsAtBan = new AtomicInteger(-1);

        static void reset() {
            writeParked = new CountDownLatch(1);
            releaseWrite = new CountDownLatch(1);
            writeReleased = new CountDownLatch(1);
            firstShardDispatchSeen.set(false);
            firstWriteSeen.set(false);
            sourceReleasedUnderWrite.set(false);
            premiseFailure.set(null);
            firstShardRequestItemCount.set(0);
            pendingChildConnectionsAtBan.set(-1);
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            coordinatorTaskManager = services.taskManager();
            return List.of();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (INDEX.equals(indexModule.getIndex().getName())) {
                indexModule.addIndexOperationListener(new ParkFirstWrite());
            }
        }

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new BanChildrenBetweenShardDispatches());
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

        /// Holds a primary write open while the rest of the bulk is dispatched, then records whether the document source it is about
        /// to index was freed underneath it.
        private static class ParkFirstWrite implements IndexingOperationListener {

            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
                final BytesReference source = index.parsedDoc().source().originalBytes();
                // An update item can reach the engine with bytes the update path produced rather than the ones the caller owns, and
                // those say nothing about whether the caller's bytes outlived the write, so wait for one that still points at them
                if (index.origin() != Engine.Operation.Origin.PRIMARY
                    || source instanceof ReleasableBytesReference == false
                    || firstWriteSeen.compareAndSet(false, true) == false) {
                    return index;
                }
                try {
                    writeParked.countDown();
                    if (awaitQuietly(releaseWrite, PARKED_WRITE_TIMEOUT) == false) {
                        recordPremiseFailure("the parked write was never released");
                    } else {
                        // hasReferences() is the only accessor that stays safe to call once the bytes have been released
                        sourceReleasedUnderWrite.set(((ReleasableBytesReference) source).hasReferences() == false);
                    }
                } finally {
                    writeReleased.countDown();
                }
                return index;
            }
        }

        /// Runs inline on the thread driving `BulkOperation`'s dispatch loop, which makes it the only hook available between two shard
        /// dispatches of the same bulk. Once the first shard is writing, it bans the bulk task's children so the next dispatch throws
        /// out of [TaskManager#registerChildConnection] exactly as it would under a real cancellation. Banning directly rather than
        /// issuing a cancel keeps the interleaving deterministic.
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
                    && INDEX.equals(shardRequest.index())
                    && firstShardDispatchSeen.compareAndSet(false, true)) {
                    firstShardRequestItemCount.set(shardRequest.items().length);
                    final TaskId parentTaskId = shardRequest.getParentTask();
                    // TransportShardBulkAction registers its primary handler with the write pool, and
                    // TransportReplicationAction#handlePrimaryRequest dispatches to it before taking a permit, so this call returns
                    // once the operation is queued rather than once it is written. That is what frees this thread to ban the parent
                    // while the first shard is still writing.
                    chain.proceed(task, action, request, listener);
                    if (awaitQuietly(writeParked, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the first shard request never reached the engine");
                    } else if (parentTaskId.isSet() == false) {
                        recordPremiseFailure("the shard request carried no parent bulk task to ban");
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
