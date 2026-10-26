/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep open for one query, as their responses list them. The coordinator holds one
 * lease per query, and every execution of the query keeps its own contexts in it, through its {@link Execution}. An
 * execution frees the contexts its rows don't read once its cut ended, and the others once its fetch ended. The lease
 * frees what is left when the query ends, whether it succeeded, failed or was cancelled. A context that a response lists
 * after its execution stopped reading it is freed at once.
 * <p>
 * The lease sends its requests as the user who ran the query, also when an administrator cancels it.
 */
public final class FetchContextLease implements Releasable {
    private final FetchContextService service;
    private final long rootTaskId;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final List<Execution> executions = new ArrayList<>();
    private boolean closed;

    /**
     * @param ownerContext restores the thread context of the user who runs the query
     */
    FetchContextLease(FetchContextService service, long rootTaskId, Supplier<ThreadContext.StoredContext> ownerContext) {
        this.service = service;
        this.rootTaskId = rootTaskId;
        this.ownerContext = ownerContext;
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
     * Frees every context, one request each. Sends the requests and doesn't wait for them. Closing twice does nothing.
     */
    @Override
    public void close() {
        Map<DiscoveryNode, List<ShardSearchContextId>> toFree = new HashMap<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            for (Execution execution : executions) {
                execution.held.forEach((node, ids) -> toFree.computeIfAbsent(node, n -> new ArrayList<>()).addAll(ids));
                execution.held.clear();
            }
            executions.clear();
        }
        service.removeLease(rootTaskId, this);
        toFree.forEach(this::free);
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
        private final Map<DiscoveryNode, List<ShardSearchContextId>> held = new HashMap<>();
        /**
         * The contexts the rows of the cut read, once the cut ended.
         */
        @Nullable
        private Set<ShardSearchContextId> read;
        private boolean fetchEnded;

        private Execution() {}

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
         * The fetch ended, so the execution reads none of its contexts anymore. Frees them.
         */
        public void fetchEnded() {
            Map<DiscoveryNode, List<ShardSearchContextId>> unread;
            synchronized (FetchContextLease.this) {
                fetchEnded = true;
                unread = takeUnread();
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
