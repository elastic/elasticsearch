/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * The reader contexts of the fetch phase of ES|QL queries, on both sides.
 * <p>
 * On a data node, a request of the query phase opens a single-session reader context per shard, like the query phase of
 * {@code _search}, and keeps the ones whose rows leave the node. On a coordinator, it keeps one {@link FetchContextLease}
 * per running query, which frees each context with the free action of {@code _search} once its execution no longer reads
 * it, and at the latest when the query ends.
 */
public final class FetchContextService {
    private static final Logger logger = LogManager.getLogger(FetchContextService.class);

    private final SearchService searchService;
    private final SearchTransportService searchTransportService;
    private final Map<Long, FetchContextLease> leases = new ConcurrentHashMap<>();

    public FetchContextService(SearchService searchService, SearchTransportService searchTransportService) {
        this.searchService = searchService;
        this.searchTransportService = searchTransportService;
    }

    /**
     * Closes the lease of a query when its task goes away, whichever action ran the query and however it ended.
     */
    public void registerTaskListener() {
        transportService().getTaskManager().registerRemovedTaskListener(task -> closeLease(task.getId()));
    }

    /**
     * The fetch contexts of one data node request.
     *
     * @param task the task of the request, recorded as the creator of its contexts
     */
    public NodeFetchContexts newNodeContexts(@Nullable Task task) {
        return new NodeFetchContexts(this, task);
    }

    /**
     * The lease of the query that runs under {@code rootTask}, created by the first execution of the query that opens fetch
     * contexts. Every later execution of the query shares it and keeps its own contexts in it, so the task frees what any
     * execution left. The lease closes when the task is cancelled or goes away. Called only while the query runs, because
     * nothing closes a lease created after its task went away.
     */
    public FetchContextLease leaseFor(CancellableTask rootTask) {
        FetchContextLease existing = leases.get(rootTask.getId());
        if (existing != null) {
            return existing;
        }
        ThreadContext threadContext = transportService().getThreadPool().getThreadContext();
        FetchContextLease created = new FetchContextLease(this, rootTask.getId(), threadContext.newRestorableContext(false));
        existing = leases.putIfAbsent(rootTask.getId(), created);
        if (existing != null) {
            return existing;
        }
        // runs at once when the task is already cancelled
        rootTask.addListener(created::close);
        return created;
    }

    /**
     * Frees every context of the query that ran under the task {@code rootTaskId}, if it opened any.
     */
    public void closeLease(long rootTaskId) {
        FetchContextLease lease = leases.get(rootTaskId);
        if (lease != null) {
            lease.close();
        }
    }

    void removeLease(long rootTaskId, FetchContextLease lease) {
        leases.remove(rootTaskId, lease);
    }

    /**
     * Frees a context on {@code node} with the free action of {@code _search}. Sends the request and doesn't wait for it.
     */
    void sendFree(DiscoveryNode node, ShardSearchContextId id) {
        Transport.Connection connection;
        try {
            connection = transportService().getConnection(node);
        } catch (Exception e) {
            // the reaper frees the context once its keep-alive passes
            logger.debug(() -> Strings.format("can't free fetch context [%s] on [%s]", id, node), e);
            return;
        }
        searchTransportService.sendFreeContext(
            connection,
            id,
            ActionListener.wrap(
                response -> {},
                e -> logger.debug(() -> Strings.format("failed to free fetch context [%s] on [%s]", id, node), e)
            )
        );
    }

    SearchService searchService() {
        return searchService;
    }

    Executor generic() {
        return transportService().getThreadPool().generic();
    }

    private TransportService transportService() {
        return searchTransportService.transportService();
    }
}
