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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
         * @param nowInNanos    the current time, from the scheduler's clock
         */
        void accumulateSearchWaiting(long waitedInNanos, long nowInNanos);
    }

    /**
     * A shard the tasks of an executor work on, and whether they still have work for it. Only shards with work accumulate
     * waiting.
     */
    public record SearchedShard(IndexShard shard, BooleanSupplier hasWork) {
        /**
         * A shard with work as long as its tasks are queued.
         */
        public static SearchedShard of(IndexShard shard) {
            return new SearchedShard(shard, () -> true);
        }
    }

    /**
     * A {@link SearchedShard}. {@code id} identifies it across the executors of a query.
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
     * Submits the tasks of a query to the executor it wraps and tracks them for the query.
     */
    public final class QueryExecutor implements Executor, Releasable {
        private final TaskId queryId;
        private final Executor delegate;
        private final List<Shard> shards;
        /** Tasks submitted through this executor and not started. Guarded by the query they were submitted for. */
        private int queuedTasks;
        /** Only checked by assertions. */
        private boolean closed;

        private QueryExecutor(TaskId queryId, Executor delegate, List<Shard> shards) {
            this.queryId = queryId;
            this.delegate = delegate;
            this.shards = shards;
        }

        @Override
        public void execute(Runnable runnable) {
            assert closed == false : "executor of query [" + queryId + "] is closed";
            QueuedTask task = new QueuedTask(this);
            onQueued(task);
            Runnable wrapped = runnable instanceof AbstractRunnable abstractRunnable
                ? new TaskAbstractRunnable(task, abstractRunnable)
                : new TaskRunnable(task, runnable);
            try {
                delegate.execute(wrapped);
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
    private void onQueued(QueuedTask task) {
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

    private void onStarted(QueuedTask task) {
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
    private void onCompleted(QueuedTask task) {
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
     * The tasks of one query on this node, guarded by itself. The clock is read only when the waiting of the query changes.
     */
    private static final class Query {
        final TaskId id;
        /** The executors of the query with tasks queued. */
        final List<QueryExecutor> queuingExecutors = new ArrayList<>(1);
        int queuedTasks;
        int runningTasks;
        /** While the query waits, the time up to which its waiting was charged to its shards. */
        long waitingChargedUntilNanos;
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

        void queued(QueryExecutor executor, LongSupplier clock) {
            boolean wasWaiting = isWaiting();
            if (executor.queuedTasks++ == 0) {
                // the shards with queued tasks change: charge the waiting so far to the shards that waited
                if (wasWaiting) {
                    chargeWaiting(clock.getAsLong());
                }
                queuingExecutors.add(executor);
            }
            queuedTasks++;
            if (wasWaiting == false && isWaiting()) {
                waitingChargedUntilNanos = clock.getAsLong();
            }
        }

        void started(QueryExecutor executor, LongSupplier clock) {
            if (isWaiting()) {
                chargeWaiting(clock.getAsLong());
            }
            removeQueued(executor);
            runningTasks++;
        }

        void completed(QueuedTask task, LongSupplier clock) {
            boolean wasWaiting = isWaiting();
            if (wasWaiting) {
                chargeWaiting(clock.getAsLong());
            }
            if (task.started) {
                runningTasks--;
            } else {
                removeQueued(task.executor);
            }
            if (wasWaiting == false && isWaiting()) {
                waitingChargedUntilNanos = clock.getAsLong();
            }
        }

        private void removeQueued(QueryExecutor executor) {
            assert executor.queuedTasks > 0 : "task of query [" + id + "] was not queued";
            queuedTasks--;
            if (--executor.queuedTasks == 0) {
                queuingExecutors.remove(executor);
            }
        }

        /**
         * Charges the time since the waiting was last charged to each distinct shard of the queued tasks that has work, so
         * once per shard the query has queued on the node.
         */
        private void chargeWaiting(long now) {
            long waited = now - waitingChargedUntilNanos;
            waitingChargedUntilNanos = now;
            if (waited <= 0) {
                return;
            }
            // an executor lists each shard once, so shards only need deduplicating across executors
            Set<Object> charged = queuingExecutors.size() > 1 ? new HashSet<>() : null;
            for (QueryExecutor executor : queuingExecutors) {
                for (Shard shard : executor.shards) {
                    if (shard.hasWork().getAsBoolean() && (charged == null || charged.add(shard.id()))) {
                        shard.searchWaiting().accumulateSearchWaiting(waited, now);
                    }
                }
            }
        }
    }

    /**
     * A task submitted through a {@link QueryExecutor}. Its query is set before it is submitted to the delegate; its flags
     * are guarded by its query.
     */
    private static final class QueuedTask {
        final QueryExecutor executor;
        Query query;
        boolean started;
        boolean completed;

        QueuedTask(QueryExecutor executor) {
            this.executor = executor;
        }
    }

    private final class TaskRunnable implements Runnable {
        private final QueuedTask task;
        private final Runnable delegate;

        TaskRunnable(QueuedTask task, Runnable delegate) {
            this.task = task;
            this.delegate = delegate;
        }

        @Override
        public void run() {
            onStarted(task);
            try {
                delegate.run();
            } finally {
                onCompleted(task);
            }
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    /**
     * Keeps the rejection, failure and force execution behavior of the {@link AbstractRunnable} it wraps.
     */
    private final class TaskAbstractRunnable extends AbstractRunnable {
        private final QueuedTask task;
        private final AbstractRunnable delegate;

        TaskAbstractRunnable(QueuedTask task, AbstractRunnable delegate) {
            this.task = task;
            this.delegate = delegate;
        }

        @Override
        public boolean isForceExecution() {
            return delegate.isForceExecution();
        }

        @Override
        protected void doRun() {
            onStarted(task);
            try {
                delegate.run();
            } finally {
                onCompleted(task);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // the delegate handles its own failures, so this was thrown by its failure handling
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException(e);
        }

        @Override
        public void onRejection(Exception e) {
            onCompleted(task);
            delegate.onRejection(e);
        }

        @Override
        public void onAfter() {
            if (task.started == false) {
                // rejected: the delegate did not run, so it did not call its own onAfter
                onCompleted(task);
                delegate.onAfter();
            }
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
