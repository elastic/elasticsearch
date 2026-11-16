/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The fetch contexts of the queries this node coordinates. It keeps one {@link FetchContextLease} per running query, which
 * frees each context once its execution no longer reads it, and at the latest when the query ends. It is the
 * {@link FetchContextTransport} of the leases, and reaches the nodes of the local cluster with the free action of
 * {@code _search}, one request per context.
 */
public final class FetchContextLeases implements FetchContextTransport {
    private static final Logger logger = LogManager.getLogger(FetchContextLeases.class);

    private final SearchService searchService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final FetchNodeDirectory nodes;
    private final Map<Long, FetchContextLease> leases = new ConcurrentHashMap<>();

    /**
     * @param nodes finds the nodes that hold the contexts of the queries this node coordinates
     */
    public FetchContextLeases(
        SearchService searchService,
        TransportService transportService,
        SearchTransportService searchTransportService,
        FetchNodeDirectory nodes
    ) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.nodes = nodes;
    }

    /**
     * The nodes that hold the fetch contexts of the queries this node coordinates.
     */
    public FetchNodeDirectory nodes() {
        return nodes;
    }

    /**
     * Closes the lease of a query when its task goes away, whichever action ran the query and however it ended.
     */
    public void registerTaskListener() {
        transportService.getTaskManager().registerRemovedTaskListener(task -> closeLease(task.getId()));
    }

    /**
     * The lease of the query that runs under {@code rootTask}, created by the first execution of the query that keeps fetch
     * contexts. Every later execution of the query shares it and keeps its own contexts in it, so the task frees what any
     * execution left. The keep-alive of the contexts is the default keep-alive of {@code _search} on this node. The lease
     * closes when the task is cancelled or goes away. Called only while the query runs, because nothing closes a lease
     * created after its task went away.
     */
    public FetchContextLease leaseFor(CancellableTask rootTask) {
        FetchContextLease existing = leases.get(rootTask.getId());
        if (existing != null) {
            return existing;
        }
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        TimeValue keepAlive = TimeValue.timeValueMillis(searchService.getDefaultKeepAliveInMillis());
        FetchContextLease created = new FetchContextLease(this, this, rootTask, threadContext.newRestorableContext(false), keepAlive);
        existing = leases.putIfAbsent(rootTask.getId(), created);
        if (existing != null) {
            return existing;
        }
        // runs at once when the task is already cancelled
        rootTask.addListener(created::close);
        return created;
    }

    /**
     * Frees every context of the query that ran under the task {@code rootTaskId}, if it kept any.
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
     * Frees the contexts with {@link SearchTransportService#sendFreeContext}, like {@code _search} frees the contexts of
     * its query phase. The free goes through the checks of {@code _search}: the node frees a context only for the user who
     * opened it.
     */
    @Override
    public void free(FetchNode node, List<ShardSearchContextId> ids) {
        ActionListener<Void> logFailure = ActionListener.wrap(
            ignored -> {},
            e -> logger.debug(() -> Strings.format("failed to free [%s] fetch contexts on [%s]", ids.size(), node), e)
        );
        nodes.find(node, logFailure.delegateFailureAndWrap((l, target) -> {
            Transport.Connection connection = transportService.getConnection(target);
            for (ShardSearchContextId id : ids) {
                searchTransportService.sendFreeContext(
                    connection,
                    id,
                    ActionListener.wrap(
                        ignored -> {},
                        e -> logger.debug(() -> Strings.format("failed to free fetch context [%s] on [%s]", id, node), e)
                    )
                );
            }
        }));
    }
}
