/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * The reader contexts of the fetch phase of ES|QL queries, on both sides.
 * <p>
 * On a data node, a request of the query phase opens a single-session reader context per shard, like the query phase of
 * {@code _search}, and keeps the ones whose rows leave the node. On a coordinator, it keeps one {@link FetchContextLease}
 * per running query, which refreshes the keep-alive of each context while the query may read it, and frees it with the
 * free action of {@code _search} once its execution no longer reads it, and at the latest when the query ends.
 */
public final class FetchContextService {
    /**
     * Refreshes the keep-alive of fetch contexts. Its requests carry the index expressions of the query, so they are
     * authorized like the fetch requests.
     */
    public static final String KEEP_ALIVE_ACTION_NAME = FetchService.FETCH_ACTION_NAME + "/keep_alive";

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
     * Serves the keep-alive requests of the coordinators on the generic pool. Dropping a reference to a context can close
     * its reader, which must not happen on a transport thread. The search pool could reject the request when it is busy,
     * which is when slow queries need their contexts the most.
     */
    public void registerHandlers() {
        transportService().registerRequestHandler(
            KEEP_ALIVE_ACTION_NAME,
            generic(),
            FetchContextKeepAliveRequest::new,
            (request, channel, task) -> {
                keepAlive(request);
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            }
        );
    }

    /**
     * The fetch contexts of one data node request.
     *
     * @param task      the task of the request, recorded as the creator of its contexts
     * @param keepAlive the keep-alive of the query, which every context gets at least
     */
    public NodeFetchContexts newNodeContexts(@Nullable Task task, TimeValue keepAlive) {
        return new NodeFetchContexts(this, task, keepAlive);
    }

    /**
     * The lease of the query that runs under {@code rootTask}, created by the first execution of the query that opens fetch
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
        ThreadContext threadContext = transportService().getThreadPool().getThreadContext();
        FetchContextLease created = new FetchContextLease(
            this,
            rootTask,
            threadContext.newRestorableContext(false),
            TimeValue.timeValueMillis(searchService.getDefaultKeepAliveInMillis())
        );
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

    /**
     * Refreshes the keep-alive of contexts on {@code node}, as a child of the task of their query. Sends the request and
     * doesn't wait for it. A refresh that fails leaves the contexts to the next one.
     */
    void sendKeepAlive(DiscoveryNode node, FetchContextKeepAliveRequest request, Task rootTask) {
        transportService().sendChildRequest(
            node,
            KEEP_ALIVE_ACTION_NAME,
            request,
            rootTask,
            TransportRequestOptions.EMPTY,
            TransportResponseHandler.empty(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.debug(
                        () -> Strings.format("failed to refresh [%s] fetch contexts on [%s]", request.contextIds().size(), node),
                        e
                    )
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
        ThreadContext threadContext = transportService().getThreadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            return transportService().getThreadPool().scheduleWithFixedDelay(refresh, interval, generic());
        } catch (EsRejectedExecutionException e) {
            logger.debug("can't refresh fetch contexts", e);
            return null;
        }
    }

    /**
     * Keeps every context of {@code request} open for at least the keep-alive of the request, from now. Skips a context
     * that is gone, isn't a fetch context, or belongs to an index the request may not read: the fetch that needs it reports
     * it. The response is the same either way, so it tells nobody which contexts exist.
     */
    void keepAlive(FetchContextKeepAliveRequest request) {
        assert Transports.assertNotTransportThread("dropping a reference to a fetch context can close its reader");
        IndicesAccessControl accessControl = ShardAccess.of(transportService().getThreadPool().getThreadContext());
        for (ShardSearchContextId id : request.contextIds()) {
            try {
                ReaderContext context = searchService.findReaderContext(id, request, null);
                // the lookup ignores the searcher id, and a scroll or a point in time isn't a context of the fetch phase
                if (context.id().equals(id) == false || context.singleSession() == false) {
                    throw new SearchContextMissingException(id);
                }
                ShardAccess.check(accessControl, context.indexShard().shardId(), KEEP_ALIVE_ACTION_NAME);
                context.markAsUsed(request.keepAlive().millis()).close();
            } catch (SearchContextMissingException | ElasticsearchSecurityException e) {
                logger.debug("can't keep fetch context [{}] alive: {}", id, e.getMessage());
            }
        }
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
