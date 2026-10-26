/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.Scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep open for one query, as their responses list them. The coordinator holds one
 * lease per query, and every execution of the query keeps its own contexts in it, through its {@link Execution}. An
 * execution frees the contexts its rows don't read once its cut ended, and the others once its fetch ended. The lease
 * frees what is left when the query ends, whether it succeeded, failed or was cancelled. A context that a response lists
 * after its execution stopped reading it is freed at once.
 * <p>
 * The data nodes open the contexts with the {@link Execution#keepAlive keep-alive} of the query. While the lease holds
 * contexts, it refreshes them every third of it, so a query that waits for a slow node doesn't lose the readers of the
 * nodes that finished first. Once nobody refreshes a context, because the query ended or its coordinator left, the context
 * expires after its keep-alive like any other.
 * <p>
 * The lease sends its requests as the user who ran the query, also when an administrator cancels it. It is thread-safe:
 * the responses of the data nodes, the fetch operator, the refresh and the task of the query call it from their own
 * threads, and it guards its executions with its own lock.
 */
public final class FetchContextLease implements Releasable {
    /**
     * Two refreshes in a row can fail or come late before a context expires.
     */
    private static final int REFRESHES_PER_KEEP_ALIVE = 3;

    private final FetchContextService service;
    private final CancellableTask rootTask;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final TimeValue keepAlive;
    private final List<Execution> executions = new ArrayList<>();
    /**
     * Runs while the lease holds contexts.
     */
    @Nullable
    private Scheduler.Cancellable refreshing;
    private boolean closed;

    /**
     * @param ownerContext restores the thread context of the user who runs the query
     * @param keepAlive    how long a context of the query stays open after its last use
     */
    FetchContextLease(
        FetchContextService service,
        CancellableTask rootTask,
        Supplier<ThreadContext.StoredContext> ownerContext,
        TimeValue keepAlive
    ) {
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.service = service;
        this.rootTask = rootTask;
        this.ownerContext = ownerContext;
        this.keepAlive = keepAlive;
    }

    /**
     * A new execution of the query, which keeps its own contexts in the lease. Several executions of one query can run
     * under the same session id, so each one needs its own.
     *
     * @param indices the index expressions of the execution, which authorize its keep-alive requests like its other requests
     */
    public Execution newExecution(OriginalIndices indices) {
        if (indices.indices().length == 0) {
            // a request without index expressions is authorized for every index
            throw new IllegalArgumentException("an execution needs the index expressions of its query");
        }
        Execution execution = new Execution(indices);
        synchronized (this) {
            if (closed == false) {
                executions.add(execution);
            }
        }
        return execution;
    }

    /**
     * Frees every context, one request each, and stops refreshing them. Sends the requests and doesn't wait for them.
     * Closing twice does nothing.
     */
    @Override
    public void close() {
        Map<DiscoveryNode, List<ShardSearchContextId>> toFree = new HashMap<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            stopRefreshing();
            for (Execution execution : executions) {
                execution.held.forEach((node, ids) -> toFree.computeIfAbsent(node, n -> new ArrayList<>()).addAll(ids));
                execution.held.clear();
            }
            executions.clear();
        }
        service.removeLease(rootTask.getId(), this);
        toFree.forEach(this::free);
    }

    /**
     * Refreshes the keep-alive of every context the lease holds, with one request per execution and node. Sends the
     * requests and doesn't wait for them. Stops refreshing once the lease holds no context.
     */
    void refresh() {
        List<Refresh> refreshes = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            for (Execution execution : executions) {
                execution.held.forEach((node, ids) -> {
                    if (ids.isEmpty() == false) {
                        refreshes.add(new Refresh(node, new FetchContextKeepAliveRequest(execution.indices, ids, keepAlive)));
                    }
                });
            }
            if (refreshes.isEmpty()) {
                // the next context the lease holds starts it again
                stopRefreshing();
                return;
            }
        }
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            for (Refresh refresh : refreshes) {
                service.sendKeepAlive(refresh.node(), refresh.request(), rootTask);
            }
        }
    }

    private record Refresh(DiscoveryNode node, FetchContextKeepAliveRequest request) {}

    /**
     * Called with the lock held, when the lease starts holding a context.
     */
    private void startRefreshing() {
        if (refreshing == null) {
            TimeValue interval = TimeValue.timeValueMillis(Math.max(1, keepAlive.millis() / REFRESHES_PER_KEEP_ALIVE));
            refreshing = service.scheduleRefresh(this::refresh, interval);
        }
    }

    /**
     * Called with the lock held.
     */
    private void stopRefreshing() {
        if (refreshing != null) {
            refreshing.cancel();
            refreshing = null;
        }
    }

    private void free(DiscoveryNode node, List<ShardSearchContextId> ids) {
        if (ids.isEmpty()) {
            return;
        }
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            for (ShardSearchContextId id : ids) {
                service.sendFree(node, id);
            }
        }
    }

    /**
     * The contexts of one execution of the query, and the ones it still reads. The lease guards it.
     */
    public final class Execution {
        private final OriginalIndices indices;
        private final Map<DiscoveryNode, List<ShardSearchContextId>> held = new HashMap<>();
        /**
         * The contexts the rows of the cut read, once the cut ended.
         */
        @Nullable
        private Set<ShardSearchContextId> read;
        private boolean fetchEnded;

        private Execution(OriginalIndices indices) {
            this.indices = Objects.requireNonNull(indices);
        }

        /**
         * How long a context of the query stays open after its last use, at least. The data nodes open the contexts of the
         * execution with it.
         */
        public TimeValue keepAlive() {
            return keepAlive;
        }

        /**
         * Takes over the contexts that the response of {@code node} lists, and frees the ones the execution no longer reads.
         */
        public void add(DiscoveryNode node, List<ShardSearchContextId> ids) {
            if (ids.isEmpty()) {
                return;
            }
            List<ShardSearchContextId> unread = new ArrayList<>();
            synchronized (FetchContextLease.this) {
                if (closed) {
                    // the query ended before this response arrived
                    unread.addAll(ids);
                } else {
                    for (ShardSearchContextId id : ids) {
                        if (reads(id)) {
                            held.computeIfAbsent(node, n -> new ArrayList<>()).add(id);
                        } else {
                            unread.add(id);
                        }
                    }
                    if (unread.size() < ids.size()) {
                        startRefreshing();
                    }
                }
            }
            free(node, unread);
        }

        /**
         * The cut ended, and its rows read only the contexts in {@code read}. Frees the others, before the fetch loads the
         * documents of the rows.
         */
        public void cutEnded(Set<ShardSearchContextId> read) {
            Map<DiscoveryNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                // a second cut would free the contexts that the rows of the first one read
                assert this.read == null && fetchEnded == false : "an execution has one cut, and it ends before the fetch";
                this.read = Set.copyOf(read);
                unread = takeUnread();
            }
            unread.forEach(FetchContextLease.this::free);
        }

        /**
         * The fetch ended, so the execution reads none of its contexts anymore. Frees them, and leaves the lease.
         */
        public void fetchEnded() {
            Map<DiscoveryNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                fetchEnded = true;
                unread = takeUnread();
                executions.remove(this);
            }
            unread.forEach(FetchContextLease.this::free);
        }

        private boolean reads(ShardSearchContextId id) {
            return fetchEnded == false && (read == null || read.contains(id));
        }

        /**
         * Stops holding the contexts the execution no longer reads, and returns them.
         */
        private Map<DiscoveryNode, List<ShardSearchContextId>> takeUnread() {
            Map<DiscoveryNode, List<ShardSearchContextId>> unread = new HashMap<>();
            for (Map.Entry<DiscoveryNode, List<ShardSearchContextId>> node : held.entrySet()) {
                Iterator<ShardSearchContextId> ids = node.getValue().iterator();
                while (ids.hasNext()) {
                    ShardSearchContextId id = ids.next();
                    if (reads(id) == false) {
                        unread.computeIfAbsent(node.getKey(), n -> new ArrayList<>()).add(id);
                        ids.remove();
                    }
                }
            }
            return unread;
        }
    }
}
