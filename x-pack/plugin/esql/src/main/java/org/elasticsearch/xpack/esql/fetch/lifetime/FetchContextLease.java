/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep open for one query, as their responses list them. The coordinator holds one
 * lease per query and frees every context in it when the query ends, whether it succeeded, failed or was cancelled. A
 * context that a response lists after that is freed at once.
 * <p>
 * The lease sends its requests as the user who ran the query, also when an administrator cancels it.
 */
public final class FetchContextLease implements Releasable {
    private final FetchContextService service;
    private final long rootTaskId;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final Map<DiscoveryNode, List<ShardSearchContextId>> held = new HashMap<>();
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
     * Takes over the contexts that the response of {@code node} lists.
     */
    public void add(DiscoveryNode node, List<ShardSearchContextId> ids) {
        if (ids.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (closed == false) {
                held.computeIfAbsent(node, n -> new ArrayList<>()).addAll(ids);
                return;
            }
        }
        // the query ended before this response arrived
        free(node, ids);
    }

    /**
     * Frees every context, one request each. Sends the requests and doesn't wait for them. Closing twice does nothing.
     */
    @Override
    public void close() {
        Map<DiscoveryNode, List<ShardSearchContextId>> toFree;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toFree = new HashMap<>(held);
            held.clear();
        }
        service.removeLease(rootTaskId, this);
        toFree.forEach(this::free);
    }

    private void free(DiscoveryNode node, List<ShardSearchContextId> ids) {
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            for (ShardSearchContextId id : ids) {
                service.sendFree(node, id);
            }
        }
    }
}
