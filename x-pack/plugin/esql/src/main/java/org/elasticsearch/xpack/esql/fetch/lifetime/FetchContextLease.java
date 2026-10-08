/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep open for one query, as their responses list them. The coordinator holds one
 * lease per query and frees every context in it when the query ends, whether it succeeded, failed or was cancelled. A
 * context that a response lists after that is freed at once. A context that the last fetch of the query read is freed by
 * its node once the node answered, so the lease forgets it then.
 * <p>
 * The lease sends its requests as the user who ran the query, also when an administrator cancels it. The data nodes
 * only let the owner of a context free it.
 */
public final class FetchContextLease implements Releasable {
    /**
     * The contexts of one node that one free request covers. A request carries the index expressions it is authorized
     * with, so contexts opened for different expressions go in different requests.
     */
    private record Group(DiscoveryNode node, OriginalIndices indices) {}

    private final FetchContextService service;
    private final long rootTaskId;
    private final TimeValue keepAlive;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final Map<Group, List<ShardSearchContextId>> held = new HashMap<>();
    private boolean closed;

    /**
     * @param ownerContext restores the thread context of the user who runs the query
     */
    FetchContextLease(
        FetchContextService service,
        long rootTaskId,
        TimeValue keepAlive,
        Supplier<ThreadContext.StoredContext> ownerContext
    ) {
        this.service = service;
        this.rootTaskId = rootTaskId;
        this.keepAlive = keepAlive;
        this.ownerContext = ownerContext;
    }

    /**
     * How long the data nodes keep the contexts of this query after their last use.
     */
    public TimeValue keepAlive() {
        return keepAlive;
    }

    /**
     * Takes over the contexts that the response of {@code node} lists.
     *
     * @param indices the index expressions of the query on that node
     */
    public void add(DiscoveryNode node, OriginalIndices indices, List<OpenContextInfo> open) {
        if (open.isEmpty()) {
            return;
        }
        Group group = new Group(node, indices);
        List<ShardSearchContextId> ids = open.stream().map(OpenContextInfo::contextId).toList();
        synchronized (this) {
            if (closed == false) {
                held.computeIfAbsent(group, g -> new ArrayList<>()).addAll(ids);
                return;
            }
        }
        // the query ended before this response arrived
        free(group, ids);
    }

    /**
     * Forgets contexts that {@code nodeId} frees itself, because the fetch request that read them told it to once it
     * answered. Closing the lease then doesn't ask the node to free them a second time. Ids the lease doesn't hold are
     * ignored.
     */
    public void forget(String nodeId, Collection<ShardSearchContextId> ids) {
        if (ids.isEmpty()) {
            return;
        }
        Set<ShardSearchContextId> freed = Set.copyOf(ids);
        synchronized (this) {
            Iterator<Map.Entry<Group, List<ShardSearchContextId>>> groups = held.entrySet().iterator();
            while (groups.hasNext()) {
                Map.Entry<Group, List<ShardSearchContextId>> group = groups.next();
                if (group.getKey().node().getId().equals(nodeId)) {
                    group.getValue().removeAll(freed);
                    if (group.getValue().isEmpty()) {
                        groups.remove();
                    }
                }
            }
        }
    }

    /**
     * Frees every context, one request per node and index expressions. Sends the requests and doesn't wait for them.
     * Closing twice does nothing.
     */
    @Override
    public void close() {
        Map<Group, List<ShardSearchContextId>> toFree;
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

    private void free(Group group, List<ShardSearchContextId> ids) {
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            service.sendFree(group.node(), new FetchFreeRequest(group.indices(), ids));
        }
    }
}
