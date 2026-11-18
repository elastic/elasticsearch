/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep for one query. A data node keeps the contexts that some rows which left it
 * read, and the coordinator finds them in the origins of the rows it receives. The coordinator holds one lease per query,
 * and every execution of the query keeps its own contexts in it, through its {@link Execution}. An execution frees the
 * contexts its rows don't read once its cut ended. A node frees a context itself once it answered the fetch request that
 * read it for the last time, and the execution lets go of it then. The execution frees the others once its fetch ended.
 * The lease frees what is left when the query ends, whether it succeeded, failed or was cancelled. A context that arrives
 * after its execution stopped reading it is freed at once.
 * <p>
 * Nothing refreshes the contexts, like between the phases of {@code _search}. A data node keeps each one for the
 * {@link Execution#keepAlive keep-alive} of the query, or a shorter one when its {@code search.max_keep_alive} is shorter.
 * Once the query ended or its coordinator left, a context that nobody freed expires after its keep-alive like any other.
 * Contexts whose rows never reach the coordinator, because the exchange finished early, wait for that too.
 * <p>
 * The lease holds the contexts of each {@link FetchNode}, and reaches their nodes through a {@link FetchContextTransport},
 * so it doesn't depend on the cluster a node is in. It sends its requests as the user who ran the query, also when an
 * administrator cancels it. It is thread-safe: the exchange sources of the coordinator, the fetch operator and the task of
 * the query call it from their own threads, and it guards its executions with its own lock.
 */
public final class FetchContextLease implements Releasable {
    private final FetchContextLeases service;
    private final FetchContextTransport transport;
    private final CancellableTask rootTask;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final TimeValue keepAlive;
    private final List<Execution> executions = new ArrayList<>();
    private boolean closed;

    /**
     * @param service      forgets the lease once it closed
     * @param transport    reaches the nodes that hold the contexts
     * @param ownerContext restores the thread context of the user who runs the query
     * @param keepAlive    how long a context of the query stays open after its last use
     */
    FetchContextLease(
        FetchContextLeases service,
        FetchContextTransport transport,
        CancellableTask rootTask,
        Supplier<ThreadContext.StoredContext> ownerContext,
        TimeValue keepAlive
    ) {
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.service = service;
        this.transport = transport;
        this.rootTask = rootTask;
        this.ownerContext = ownerContext;
        this.keepAlive = keepAlive;
    }

    /**
     * A new execution of the query, which keeps its own contexts in the lease. Several executions of one query can run
     * under the same session id, so each one needs its own.
     */
    public Execution newExecution() {
        Execution execution = new Execution();
        synchronized (this) {
            if (closed == false) {
                executions.add(execution);
            }
        }
        return execution;
    }

    /**
     * Frees every context. Sends the requests and doesn't wait for them. Closing twice does nothing.
     */
    @Override
    public void close() {
        Map<FetchNode, List<ShardSearchContextId>> toFree = new HashMap<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            for (Execution execution : executions) {
                execution.takeHeld(toFree);
            }
            executions.clear();
        }
        service.removeLease(rootTask.getId(), this);
        free(toFree);
    }

    /**
     * Frees {@code toFree} as the user who ran the query. Called without the lock.
     */
    private void free(Map<FetchNode, List<ShardSearchContextId>> toFree) {
        if (toFree.isEmpty()) {
            return;
        }
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            toFree.forEach(transport::free);
        }
    }

    /**
     * The contexts of one execution of the query, and the ones it still reads. The lease guards it.
     */
    public final class Execution {
        private final Map<FetchNode, Set<ShardSearchContextId>> held = new HashMap<>();
        /**
         * The contexts that arrived after the execution stopped reading them, and were freed then. Rows of the same
         * context can keep arriving, and one free is enough.
         */
        private final Set<ShardSearchContextId> freedOnArrival = new HashSet<>();
        /**
         * The contexts the rows of the cut read, once the cut ended.
         */
        @Nullable
        private Set<ShardSearchContextId> read;
        private boolean fetchEnded;

        private Execution() {}

        /**
         * How long a context of the query stays open after its last use. The data nodes keep the contexts of the execution
         * for it, or for their {@code search.max_keep_alive} when that is shorter.
         */
        public TimeValue keepAlive() {
            return keepAlive;
        }

        /**
         * Rows read through the context {@code id} of {@code node} reached the coordinator, so the node keeps it. Holds it
         * while the execution may read it, and frees it otherwise.
         */
        public void arrived(FetchNode node, ShardSearchContextId id) {
            synchronized (FetchContextLease.this) {
                if (closed == false && reads(id)) {
                    held.computeIfAbsent(node, n -> new HashSet<>()).add(id);
                    return;
                }
                if (freedOnArrival.add(id) == false) {
                    return;
                }
            }
            // the query ended, or the execution stopped reading the context, before its rows arrived
            free(Map.of(node, List.of(id)));
        }

        /**
         * {@code node} freed the contexts {@code ids} itself, once it answered the fetch request that read them for the last
         * time. The execution stops holding them, so it doesn't free them.
         */
        public void freedByTheirNode(FetchNode node, Collection<ShardSearchContextId> ids) {
            if (ids.isEmpty()) {
                return;
            }
            synchronized (FetchContextLease.this) {
                Set<ShardSearchContextId> held = this.held.get(node);
                if (held != null) {
                    held.removeAll(ids);
                }
            }
        }

        /**
         * The cut ended, and its rows read only the contexts in {@code read}. Frees the others, before the fetch loads the
         * documents of the rows.
         */
        public void cutEnded(Set<ShardSearchContextId> read) {
            Map<FetchNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                // a second cut would free the contexts that the rows of the first one read
                assert this.read == null && fetchEnded == false : "an execution has one cut, and it ends before the fetch";
                this.read = Set.copyOf(read);
                unread = takeUnread();
            }
            free(unread);
        }

        /**
         * Hands the contexts that the rows which left read over to the coordinator of the querying cluster, and frees the
         * others. For the coordinator of a remote cluster, once its request ended: the querying cluster finds those contexts
         * in the origins of the rows it receives, and fetches from and frees them from then on. The execution holds none of
         * them after, and leaves the lease. A context that arrives later is freed at once.
         *
         * @param read the contexts the rows that left read
         */
        public void handOver(Set<ShardSearchContextId> read) {
            Map<FetchNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                assert this.read == null && fetchEnded == false : "an execution hands its contexts over once, without a fetch";
                this.read = Set.copyOf(read);
                unread = takeUnread();
                held.clear();
                fetchEnded = true;
                executions.remove(this);
            }
            free(unread);
        }

        /**
         * The fetch ended, so the execution reads none of its contexts anymore. Frees them, and leaves the lease.
         */
        public void fetchEnded() {
            Map<FetchNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                fetchEnded = true;
                unread = takeUnread();
                executions.remove(this);
            }
            free(unread);
        }

        private boolean reads(ShardSearchContextId id) {
            return fetchEnded == false && (read == null || read.contains(id));
        }

        /**
         * Stops holding the contexts the execution no longer reads, and returns them.
         */
        private Map<FetchNode, List<ShardSearchContextId>> takeUnread() {
            Map<FetchNode, List<ShardSearchContextId>> unread = new HashMap<>();
            for (Map.Entry<FetchNode, Set<ShardSearchContextId>> node : held.entrySet()) {
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

        /**
         * Stops holding every context, and adds them to {@code taken}.
         */
        private void takeHeld(Map<FetchNode, List<ShardSearchContextId>> taken) {
            held.forEach((node, ids) -> {
                if (ids.isEmpty() == false) {
                    taken.computeIfAbsent(node, n -> new ArrayList<>()).addAll(ids);
                }
            });
            held.clear();
        }
    }
}
