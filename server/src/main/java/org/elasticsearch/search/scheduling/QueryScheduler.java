/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Measures the search waiting of the queries on this node. It only measures: tasks go to their pools unchanged and in
 * order. A query waits while it has tasks queued on the node and none running: a query running fewer tasks than it queued
 * is not waiting. While a query waits, each shard with queued work accumulates the waiting time once, however many of its
 * tasks are queued.
 */
public final class QueryScheduler {

    /**
     * Accumulates the search waiting of one shard.
     */
    @FunctionalInterface
    interface SearchWaitingAccumulator {
        /**
         * @param waitedInNanos the time the shard's work waited, in nanoseconds
         * @param nowInNanos    the time the waiting was measured up to, from the scheduler's clock
         */
        void accumulateSearchWaiting(long waitedInNanos, long nowInNanos);
    }

    /**
     * A shard the tasks of an executor work on, and whether they still have work for it. Only shards with work accumulate
     * waiting.
     */
    public record SearchedShard(IndexShard shard, BooleanSupplier hasWork) {}

    /**
     * A shard as the scheduler tracks it. {@code id} identifies it across the executors of a query.
     */
    record Shard(Object id, SearchWaitingAccumulator searchWaiting, BooleanSupplier hasWork) {}

    private final LongSupplier clock;
    private final ConcurrentHashMap<TaskId, Query> queries = new ConcurrentHashMap<>();

    /**
     * @param clock the clock waiting is measured with, like {@link System#nanoTime()}
     */
    public QueryScheduler(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Returns an executor that submits the tasks of the query {@code queryId} to {@code delegate}. The tasks work on
     * {@code shards}, each listed once, which accumulate the waiting into {@link ShardSearchStats#accumulateSearchWaiting}.
     * A query is identified by the task of its coordinator in this cluster, the same on every node.
     */
    public QueryExecutor executor(TaskId queryId, Executor delegate, List<SearchedShard> shards) {
        List<Shard> searched = new ArrayList<>(shards.size());
        for (SearchedShard shard : shards) {
            ShardSearchStats stats = shard.shard().shardSearchStats();
            searched.add(new Shard(stats, stats::accumulateSearchWaiting, shard.hasWork()));
        }
        return newExecutor(queryId, delegate, searched);
    }

    /**
     * Returns an executor that submits the tasks of the query {@code queryId} to {@code delegate}, all working on
     * {@code shard}, which has work as long as they are queued.
     */
    public QueryExecutor executor(TaskId queryId, Executor delegate, IndexShard shard) {
        ShardSearchStats stats = shard.shardSearchStats();
        return newExecutor(queryId, delegate, List.of(new Shard(stats, stats::accumulateSearchWaiting, () -> true)));
    }

    QueryExecutor newExecutor(TaskId queryId, Executor delegate, List<Shard> shards) {
        assert shards.stream().map(Shard::id).distinct().count() == shards.size() : "shards listed more than once " + shards;
        return new QueryExecutor(queryId, delegate, shards);
    }

    /**
     * The number of queries with tasks queued or running on this node.
     */
    public int queries() {
        return queries.size();
    }

    /**
     * Submits the tasks of a query to the executor it wraps and tracks them for the query. Its state is guarded by the
     * query its tasks are submitted for.
     */
    public final class QueryExecutor implements Executor, Releasable {
        private final TaskId queryId;
        private final Executor delegate;
        private final List<Shard> shards;
        /** Tasks submitted and not started. */
        private int queuedTasks;
        /** The waiting of the query when this executor last charged its shards, or started queuing. */
        private long chargedWaitedNanos;
        /** The state of each of {@link #shards} in the query, while {@link #shared}. */
        private QueuedShard[] queuedShards;
        /** Whether other executors of the query queued at the same time, so its shards are charged through the query. */
        private boolean shared;
        /** Only checked by assertions. */
        private boolean closed;

        private QueryExecutor(TaskId queryId, Executor delegate, List<Shard> shards) {
            this.queryId = queryId;
            this.delegate = delegate;
            this.shards = shards;
        }

        @Override
        public void execute(Runnable runnable) {
            execute(runnable, delegate);
        }

        /**
         * Submits a task to {@code via} instead of the executor this one wraps, tracked like the other tasks of this
         * executor.
         */
        public void execute(Runnable runnable, Executor via) {
            assert closed == false : "executor of query [" + queryId + "] is closed";
            Task task = new Task(this, runnable);
            onQueued(task);
            try {
                via.execute(runnable instanceof AbstractRunnable ? task : new PlainTask(task));
            } catch (RuntimeException e) {
                onCompleted(task);
                throw e;
            }
        }

        /**
         * The executor must not submit more tasks. The tasks it submitted are tracked until they complete.
         */
        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Counts the task as queued for its query, tracking the query if it has no other task.
     */
    private void onQueued(Task task) {
        TaskId queryId = task.executor.queryId;
        while (true) {
            Query query = queries.computeIfAbsent(queryId, Query::new);
            synchronized (query) {
                if (query.forgotten) {
                    // its last task completed meanwhile: track the query again
                    queries.remove(queryId, query);
                    continue;
                }
                query.queued(task.executor, clock);
            }
            task.query = query;
            return;
        }
    }

    private void onStarted(Task task) {
        synchronized (task.query) {
            task.started = true;
            // a task its delegate queued before throwing was completed when the delegate threw
            if (task.completed == false) {
                task.query.started(task.executor, clock);
            }
        }
    }

    /**
     * The task completed, or was rejected before it started. Only the first call for a task counts.
     */
    private void onCompleted(Task task) {
        Query query = task.query;
        synchronized (query) {
            if (task.completed) {
                return;
            }
            task.completed = true;
            query.completed(task, clock);
            if (query.isIdle() == false) {
                return;
            }
            query.forgotten = true;
        }
        queries.remove(query.id, query);
    }

    /**
     * The tasks of one query on this node, guarded by itself.
     * <p>
     * The query keeps the total time it waited on the node. The shards of a task are charged the part of that total they
     * were not charged yet when the task leaves the queue, so a task never updates the other shards of the query. The
     * executor keeps what its shards were charged up to, unless other executors of the query queue at the same time: then
     * each shard keeps it in the query, so a shard they share is charged once.
     */
    private static final class Query {
        final TaskId id;
        int queuedTasks;
        int runningTasks;
        /** The time the query waited on the node, up to {@link #waitedUpToNanos}. */
        long waitedNanos;
        /** The last time {@link #waitedNanos} was brought up to date. */
        long waitedUpToNanos;
        /** The executors of the query with tasks queued. */
        int queuingExecutors;
        /** The executor with tasks queued while it is the only one, until another one queues. */
        QueryExecutor loneExecutor;
        /** The shards of the shared executors, by id. */
        Map<Object, QueuedShard> queuedShards;
        /** Whether the query had no task left and is no longer tracked. */
        boolean forgotten;

        Query(TaskId id) {
            this.id = id;
        }

        boolean isWaiting() {
            return runningTasks == 0 && queuedTasks > 0;
        }

        boolean isIdle() {
            return runningTasks == 0 && queuedTasks == 0;
        }

        /**
         * Brings {@link #waitedNanos} up to now. Called before a change that can start or stop the waiting, and before the
         * waiting is read while the query waits, so the clock is read only then.
         */
        private void tick(LongSupplier clock) {
            long now = clock.getAsLong();
            if (isWaiting()) {
                waitedNanos += now - waitedUpToNanos;
            }
            waitedUpToNanos = now;
        }

        void queued(QueryExecutor executor, LongSupplier clock) {
            boolean joins = executor.queuedTasks++ == 0;
            // the query starts waiting, or an executor starts queuing while it waits
            if (runningTasks == 0 && (queuedTasks == 0 || joins)) {
                tick(clock);
            }
            queuedTasks++;
            if (joins) {
                join(executor);
            }
        }

        void started(QueryExecutor executor, LongSupplier clock) {
            // the query stops waiting
            if (runningTasks == 0) {
                tick(clock);
            }
            queuedTasks--;
            runningTasks++;
            leaveQueue(executor);
        }

        void completed(Task task, LongSupplier clock) {
            if (task.started) {
                // the query starts waiting
                if (runningTasks == 1 && queuedTasks > 0) {
                    tick(clock);
                }
                runningTasks--;
            } else {
                // rejected before it started: it leaves the queue while the query may wait
                if (runningTasks == 0) {
                    tick(clock);
                }
                queuedTasks--;
                leaveQueue(task.executor);
            }
        }

        /**
         * The executor has its first task queued: its shards will be charged from the waiting so far, or from what a shard
         * was charged up to if another executor of the query has it queued already.
         */
        private void join(QueryExecutor executor) {
            executor.chargedWaitedNanos = waitedNanos;
            if (queuingExecutors++ == 0) {
                loneExecutor = executor;
                return;
            }
            if (loneExecutor != null) {
                share(loneExecutor);
                loneExecutor = null;
            }
            share(executor);
        }

        /**
         * Moves what the shards of the executor were charged up to into the query, where the other executors that queue the
         * same shards find it.
         */
        private void share(QueryExecutor executor) {
            if (queuedShards == null) {
                queuedShards = new HashMap<>();
            }
            if (executor.queuedShards == null) {
                executor.queuedShards = new QueuedShard[executor.shards.size()];
            }
            for (int i = 0; i < executor.shards.size(); i++) {
                Object id = executor.shards.get(i).id();
                QueuedShard queued = queuedShards.get(id);
                if (queued == null) {
                    queued = new QueuedShard(executor.chargedWaitedNanos);
                    queuedShards.put(id, queued);
                }
                queued.executors++;
                executor.queuedShards[i] = queued;
            }
            executor.shared = true;
        }

        /**
         * A task of the executor left the queue: charges its shards if the query waited since the executor last charged
         * them, and forgets them if it was the last task the executor had queued.
         */
        private void leaveQueue(QueryExecutor executor) {
            if (executor.chargedWaitedNanos != waitedNanos) {
                charge(executor);
            }
            if (--executor.queuedTasks > 0) {
                return;
            }
            queuingExecutors--;
            if (executor.shared == false) {
                assert loneExecutor == executor : "executor of query [" + id + "] is neither alone nor shared";
                loneExecutor = null;
                return;
            }
            for (int i = 0; i < executor.queuedShards.length; i++) {
                QueuedShard queued = executor.queuedShards[i];
                executor.queuedShards[i] = null;
                if (--queued.executors == 0) {
                    queuedShards.remove(executor.shards.get(i).id());
                }
            }
            executor.shared = false;
        }

        /**
         * Charges the shards of the executor that have work the waiting since they were last charged. A shard without work
         * leaves its waiting to the other executors of the query that work on it.
         */
        private void charge(QueryExecutor executor) {
            for (int i = 0; i < executor.shards.size(); i++) {
                Shard shard = executor.shards.get(i);
                if (shard.hasWork().getAsBoolean() == false) {
                    continue;
                }
                long chargedWaitedNanos;
                if (executor.shared) {
                    chargedWaitedNanos = executor.queuedShards[i].chargedWaitedNanos;
                    executor.queuedShards[i].chargedWaitedNanos = waitedNanos;
                } else {
                    chargedWaitedNanos = executor.chargedWaitedNanos;
                }
                if (waitedNanos > chargedWaitedNanos) {
                    shard.searchWaiting().accumulateSearchWaiting(waitedNanos - chargedWaitedNanos, waitedUpToNanos);
                }
            }
            executor.chargedWaitedNanos = waitedNanos;
        }
    }

    /**
     * A shard of the shared executors of a query.
     */
    private static final class QueuedShard {
        /** The waiting of the query the shard was charged up to. */
        long chargedWaitedNanos;
        /** The shared executors with tasks queued for the shard. */
        int executors;

        QueuedShard(long chargedWaitedNanos) {
            this.chargedWaitedNanos = chargedWaitedNanos;
        }
    }

    /**
     * A task submitted through a {@link QueryExecutor}. It keeps the rejection, failure and force execution behavior of the
     * {@link AbstractRunnable} it wraps. Its query is set before it is submitted to the delegate; its flags are guarded by
     * its query.
     */
    private final class Task extends AbstractRunnable {
        final QueryExecutor executor;
        final Runnable delegate;
        Query query;
        boolean started;
        boolean completed;

        Task(QueryExecutor executor, Runnable delegate) {
            this.executor = executor;
            this.delegate = delegate;
        }

        @Override
        public boolean isForceExecution() {
            return delegate instanceof AbstractRunnable abstractRunnable && abstractRunnable.isForceExecution();
        }

        @Override
        protected void doRun() {
            onStarted(this);
            try {
                delegate.run();
            } finally {
                onCompleted(this);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // thrown by a plain runnable, or by the failure handling of an AbstractRunnable: rethrow it as they would unwrapped
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException(e);
        }

        @Override
        public void onRejection(Exception e) {
            onCompleted(this);
            ((AbstractRunnable) delegate).onRejection(e);
        }

        @Override
        public void onAfter() {
            if (started == false) {
                // rejected: the delegate did not run, so it did not call its own onAfter
                onCompleted(this);
                ((AbstractRunnable) delegate).onAfter();
            }
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    /**
     * A {@link Task} that wraps a plain {@link Runnable}, submitted as a plain {@link Runnable} so the executor handles it,
     * and rejects it, as it would the runnable.
     */
    private record PlainTask(Task task) implements Runnable {
        @Override
        public void run() {
            task.run();
        }

        @Override
        public String toString() {
            return task.toString();
        }
    }
}
