/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.Response;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Runs the reads of stored result pages that wait on this node. Pages are routed by result id, so the node sees every page of
 * the results its shards hold as soon as it is indexed, before any refresh, and wakes the reads waiting for them.
 * <p>
 * The reads of a shard share its local refreshes: one runs at a time and the reads that ask meanwhile share the next one. At
 * most {@link #MAX_WAITING_READS} reads wait on a node at a time.
 */
public final class StoredPagesService implements IndexingOperationListener, IndexEventListener {

    static final int MAX_WAITING_READS = 1000;

    private static final Logger logger = LogManager.getLogger(StoredPagesService.class);

    private final Map<String, List<Waiter>> waiters = new ConcurrentHashMap<>();
    private final AtomicInteger waitingReads = new AtomicInteger();
    private final Map<ShardId, SingleResultDeduplicator<Void>> refreshes = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private final Executor executor;
    private final BiConsumer<ShardId, ActionListener<Void>> refresh;
    private volatile Consumer<String> waitingListener = streamId -> {};

    /**
     * @param executor runs the reads
     * @param refresh  refreshes a shard locally, off the calling thread
     */
    public StoredPagesService(ThreadPool threadPool, Executor executor, BiConsumer<ShardId, ActionListener<Void>> refresh) {
        this.threadPool = threadPool;
        this.executor = executor;
        this.refresh = refresh;
    }

    /**
     * Answers with {@code read} if it finds pages or the end of the result. Otherwise waits, up to {@code timeout} from now, for a
     * document of the result to be indexed on this node, then refreshes the shard and answers with a new read. A read that times
     * out answers with what is visible. The wait fails if its shard closes, or at once if too many reads wait on this node; a read
     * counts as waiting until it answers. A wait cannot be cancelled: an abandoned read holds its slot until it times out.
     */
    void read(
        ShardId shardId,
        String streamId,
        CheckedSupplier<Response, Exception> read,
        TimeValue timeout,
        ActionListener<Response> listener
    ) throws Exception {
        Response visible = read.get();
        if (visible.answers() || timeout.millis() <= 0) {
            ActionListener.respondAndRelease(listener, visible);
            return;
        }
        visible.decRef();
        if (waitingReads.incrementAndGet() > MAX_WAITING_READS) {
            waitingReads.decrementAndGet();
            listener.onFailure(new EsRejectedExecutionException("too many reads wait for stored result pages on this node", false));
            return;
        }
        Waiter waiter = new Waiter(shardId, streamId, read, ActionListener.runAfter(listener, waitingReads::decrementAndGet));
        // registered before the refresh: a page indexed after it wakes the read, one indexed before it is visible
        waiters.compute(streamId, (id, list) -> {
            List<Waiter> next = list == null ? new ArrayList<>(1) : new ArrayList<>(list);
            next.add(waiter);
            return next;
        });
        waiter.scheduleTimeout(timeout);
        refresh(shardId, ActionListener.wrap(v -> executor.execute(waiter.readAfterRefresh()), waiter::fail));
    }

    /**
     * Calls {@code listener} with the result id each time a read on this node starts waiting. For tests.
     */
    public void setWaitingListener(Consumer<String> listener) {
        waitingListener = listener;
    }

    /**
     * The number of reads waiting on this node.
     */
    public int waitingReads() {
        return waitingReads.get();
    }

    /**
     * Makes visible what was indexed on the shard before this call, with a refresh that starts after it.
     */
    void refresh(ShardId shardId, ActionListener<Void> listener) {
        refreshes.computeIfAbsent(shardId, id -> new SingleResultDeduplicator<>(threadPool.getThreadContext(), l -> refresh.accept(id, l)))
            .execute(listener);
    }

    @Override
    public IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
        // the default materializes every operation of the batch
        return batch;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (waiters.isEmpty() == false && index.routing() != null) {
            wake(index.routing());
        }
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        BytesRef[] routings = batch.routings();
        if (waiters.isEmpty() || routings == null) {
            return;
        }
        // a batch usually holds the pages of a single result
        Set<String> woken = new HashSet<>(2);
        for (int i = 0; i < batch.docCount(); i++) {
            BytesRef routing = routings[batch.offset() + i];
            if (routing != null) {
                String streamId = routing.utf8ToString();
                if (woken.add(streamId)) {
                    wake(streamId);
                }
            }
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        refreshes.remove(shardId);
        for (List<Waiter> list : waiters.values()) {
            for (Waiter waiter : list) {
                if (waiter.shardId.equals(shardId)) {
                    waiter.fail(new IndexShardClosedException(shardId));
                }
            }
        }
    }

    void wake(String streamId) {
        List<Waiter> toWake = waiters.remove(streamId);
        if (toWake == null) {
            return;
        }
        for (Waiter waiter : toWake) {
            try {
                waiter.wake.run();
            } catch (Exception e) {
                // a failed wake-up must not fail the indexing operation
                logger.warn(() -> "failed to wake a read of stored result [" + streamId + "]", e);
            }
        }
    }

    /**
     * A read waiting for pages. The first of its wake-up, timeout or failure ends the wait and answers; the others do nothing.
     */
    private final class Waiter {
        private final ShardId shardId;
        private final String streamId;
        private final CheckedSupplier<Response, Exception> read;
        private final ActionListener<Response> listener;
        // called on the indexing thread, in the context of the bulk
        private final Runnable wake;
        private final AtomicBoolean done = new AtomicBoolean();
        private volatile Scheduler.Cancellable scheduledTimeout;

        Waiter(ShardId shardId, String streamId, CheckedSupplier<Response, Exception> read, ActionListener<Response> listener) {
            this.shardId = shardId;
            this.streamId = streamId;
            this.read = read;
            this.listener = listener;
            this.wake = threadPool.getThreadContext().preserveContext(() -> {
                if (end()) {
                    refresh(shardId, listener.delegateFailureAndWrap((l, v) -> executor.execute(ActionRunnable.supplyAndDecRef(l, read))));
                }
            });
        }

        /**
         * Whether this call ends the wait, and so must answer.
         */
        private boolean end() {
            if (done.compareAndSet(false, true) == false) {
                return false;
            }
            waiters.computeIfPresent(streamId, (id, list) -> {
                List<Waiter> next = new ArrayList<>(list);
                next.remove(this);
                return next.isEmpty() ? null : next;
            });
            Scheduler.Cancellable scheduled = scheduledTimeout;
            if (scheduled != null) {
                scheduled.cancel();
            }
            return true;
        }

        void fail(Exception e) {
            if (end()) {
                listener.onFailure(e);
            }
        }

        AbstractRunnable readAfterRefresh() {
            return new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    Response response = read.get();
                    try {
                        if (response.answers() == false) {
                            if (done.get() == false) {
                                waitingListener.accept(streamId);
                            }
                        } else if (end()) {
                            listener.onResponse(response);
                        }
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            };
        }

        void scheduleTimeout(TimeValue timeout) {
            AbstractRunnable onTimeout = new AbstractRunnable() {
                @Override
                protected void doRun() {
                    if (end()) {
                        ActionRunnable.supplyAndDecRef(listener, read).run();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }

                @Override
                public boolean isForceExecution() {
                    // nothing else ends a wait whose result gets no more documents
                    return true;
                }
            };
            scheduledTimeout = threadPool.schedule(() -> executor.execute(onTimeout), timeout, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            if (done.get()) {
                // ended before the timeout was set
                scheduledTimeout.cancel();
            }
        }
    }
}
