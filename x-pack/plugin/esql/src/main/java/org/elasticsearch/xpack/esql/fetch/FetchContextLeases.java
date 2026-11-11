/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The fetch contexts of the queries this node coordinates. It keeps one {@link FetchContextLease} per running query, which
 * refreshes the keep-alive of each context while the query may read it, and frees it once its execution no longer reads it,
 * and at the latest when the query ends. It is the {@link FetchContextTransport} of the leases, and reaches the nodes of the
 * local cluster and of remote clusters, with one request for the contexts of an execution on a node. The data nodes serve
 * those requests with {@link FetchContextService}.
 */
public final class FetchContextLeases implements FetchContextTransport {
    private static final Logger logger = LogManager.getLogger(FetchContextLeases.class);

    private final SearchService searchService;
    private final TransportService transportService;
    private final FetchNodeDirectory nodes;
    private final Map<Long, FetchContextLease> leases = new ConcurrentHashMap<>();

    /**
     * @param nodes finds the nodes that hold the contexts of the queries this node coordinates
     */
    public FetchContextLeases(SearchService searchService, TransportService transportService, FetchNodeDirectory nodes) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.nodes = nodes;
    }

    /**
     * The nodes that hold the fetch contexts of the queries this node coordinates, in its own cluster and in remote ones.
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
     * The lease of the query that runs under {@code rootTask}, created by the first execution of the query that opens fetch
     * contexts. Every later execution of the query shares it and keeps its own contexts in it, so the task frees what any
     * execution left. The keep-alive of the contexts is the default keep-alive of {@code _search} on this node. The lease
     * closes when the task is cancelled or goes away. Called only while the query runs, because nothing closes a lease
     * created after its task went away.
     */
    public FetchContextLease leaseFor(CancellableTask rootTask) {
        return leaseFor(rootTask, TimeValue.timeValueMillis(searchService.getDefaultKeepAliveInMillis()));
    }

    /**
     * The lease of the request of a querying cluster that runs under {@code rootTask} on this node, the coordinator of a
     * remote cluster, like {@link #leaseFor(CancellableTask)}. Its contexts get the keep-alive {@code keepAlive} of the
     * query, which the coordinator of the querying cluster refreshes them with once it holds them.
     */
    public FetchContextLease leaseFor(CancellableTask rootTask, TimeValue keepAlive) {
        FetchContextLease existing = leases.get(rootTask.getId());
        if (existing != null) {
            return existing;
        }
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
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
     * Frees the contexts with one request of {@link FetchContextService#FREE_ACTION_NAME}.
     */
    @Override
    public void free(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids) {
        ActionListener<Void> logFailure = ActionListener.wrap(
            ignored -> {},
            e -> logger.debug(() -> Strings.format("failed to free [%s] fetch contexts on [%s]", ids.size(), node), e)
        );
        nodes.find(
            node,
            logFailure.delegateFailureAndWrap(
                (l, target) -> transportService.sendRequest(
                    FetchNodeDirectory.connection(transportService, node, target),
                    FetchContextService.FREE_ACTION_NAME,
                    new FetchContextFreeRequest(indices, ids),
                    TransportRequestOptions.EMPTY,
                    TransportResponseHandler.empty(EsExecutors.DIRECT_EXECUTOR_SERVICE, l)
                )
            )
        );
    }

    /**
     * Sends one keep-alive request for the contexts. A refresh that fails leaves the contexts to the next one.
     */
    @Override
    public void keepAlive(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids, TimeValue keepAlive, Task rootTask) {
        ActionListener<Void> logFailure = ActionListener.wrap(
            ignored -> {},
            e -> logger.debug(() -> Strings.format("failed to refresh [%s] fetch contexts on [%s]", ids.size(), node), e)
        );
        nodes.find(
            node,
            logFailure.delegateFailureAndWrap(
                (l, target) -> transportService.sendChildRequest(
                    FetchNodeDirectory.connection(transportService, node, target),
                    FetchContextService.KEEP_ALIVE_ACTION_NAME,
                    new FetchContextKeepAliveRequest(indices, ids, keepAlive),
                    rootTask,
                    TransportRequestOptions.EMPTY,
                    TransportResponseHandler.empty(EsExecutors.DIRECT_EXECUTOR_SERVICE, l)
                )
            )
        );
    }

    /**
     * Runs {@code refresh} on the generic pool every {@code interval}, outside the thread context of the caller.
     *
     * @return {@code null} when the node is shutting down. The contexts then live until their keep-alive passes.
     */
    @Nullable
    Scheduler.Cancellable scheduleRefresh(Runnable refresh, TimeValue interval) {
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            return transportService.getThreadPool().scheduleWithFixedDelay(refresh, interval, transportService.getThreadPool().generic());
        } catch (EsRejectedExecutionException e) {
            logger.debug("can't refresh fetch contexts", e);
            return null;
        }
    }
}
