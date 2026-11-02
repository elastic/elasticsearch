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
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * The reader contexts of the fetch phase of ES|QL queries, on both sides.
 * <p>
 * On a data node, a request of the query phase opens a single-session reader context per shard, like the query phase of
 * {@code _search}, and keeps the ones whose rows leave the node. It serves the keep-alive and free requests of the
 * coordinators. On a coordinator, it keeps one {@link FetchContextLease} per running query, which refreshes the keep-alive
 * of each context while the query may read it, and frees it once its execution no longer reads it, and at the latest when
 * the query ends. It is the {@link FetchContextTransport} of the leases, and reaches the nodes of the local cluster, with
 * one request for the contexts of an execution on a node.
 */
public final class FetchContextService implements FetchContextTransport {
    /**
     * Refreshes the keep-alive of fetch contexts. Its requests carry the index expressions of the query, so they are
     * authorized like the fetch requests.
     */
    public static final String KEEP_ALIVE_ACTION_NAME = FetchService.FETCH_ACTION_NAME + "/keep_alive";

    /**
     * Frees fetch contexts. Its requests carry the index expressions of the query, so they are authorized like the fetch
     * requests.
     */
    public static final String FREE_ACTION_NAME = FetchService.FETCH_ACTION_NAME + "/free";

    private static final Logger logger = LogManager.getLogger(FetchContextService.class);

    private final SearchService searchService;
    private final TransportService transportService;
    private final Supplier<DiscoveryNodes> nodes;
    private final Map<Long, FetchContextLease> leases = new ConcurrentHashMap<>();

    /**
     * @param nodes the nodes of the local cluster, as of now
     */
    public FetchContextService(SearchService searchService, TransportService transportService, Supplier<DiscoveryNodes> nodes) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.nodes = nodes;
    }

    /**
     * Closes the lease of a query when its task goes away, whichever action ran the query and however it ended.
     */
    public void registerTaskListener() {
        transportService.getTaskManager().registerRemovedTaskListener(task -> closeLease(task.getId()));
    }

    /**
     * Serves the keep-alive and free requests of the coordinators on the generic pool. Dropping a reference to a context
     * can close its reader, which must not happen on a transport thread. The search pool could reject the request when it
     * is busy, which is when slow queries need their contexts the most, and when freeing them helps the most.
     */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            KEEP_ALIVE_ACTION_NAME,
            generic(),
            FetchContextKeepAliveRequest::new,
            (request, channel, task) -> {
                serveKeepAlive(request);
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            }
        );
        transportService.registerRequestHandler(FREE_ACTION_NAME, generic(), FetchContextFreeRequest::new, (request, channel, task) -> {
            serveFree(request);
            channel.sendResponse(ActionResponse.Empty.INSTANCE);
        });
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
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        FetchContextLease created = new FetchContextLease(
            this,
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
     * Frees the contexts with one request of {@link #FREE_ACTION_NAME}.
     */
    @Override
    public void free(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids) {
        DiscoveryNode target = discoveryNode(node);
        if (target == null) {
            return;
        }
        transportService.sendRequest(
            target,
            FREE_ACTION_NAME,
            new FetchContextFreeRequest(indices, ids),
            TransportRequestOptions.EMPTY,
            TransportResponseHandler.empty(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.debug(() -> Strings.format("failed to free [%s] fetch contexts on [%s]", ids.size(), target), e)
                )
            )
        );
    }

    /**
     * Sends one keep-alive request for the contexts. A refresh that fails leaves the contexts to the next one.
     */
    @Override
    public void keepAlive(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids, TimeValue keepAlive, Task rootTask) {
        DiscoveryNode target = discoveryNode(node);
        if (target == null) {
            return;
        }
        transportService.sendChildRequest(
            target,
            KEEP_ALIVE_ACTION_NAME,
            new FetchContextKeepAliveRequest(indices, ids, keepAlive),
            rootTask,
            TransportRequestOptions.EMPTY,
            TransportResponseHandler.empty(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.debug(() -> Strings.format("failed to refresh [%s] fetch contexts on [%s]", ids.size(), target), e)
                )
            )
        );
    }

    /**
     * The node {@code node} names in the local cluster, or {@code null} when it left the cluster, and its contexts with it.
     */
    @Nullable
    private DiscoveryNode discoveryNode(FetchNode node) {
        if (node.isLocal() == false) {
            assert false : "the fetch reads the local cluster only, but a lease holds contexts of [" + node + "]";
            logger.warn("can't reach the fetch contexts of [{}] on another cluster, they expire after their keep-alive", node);
            return null;
        }
        DiscoveryNode found = nodes.get().get(node.nodeId());
        if (found == null) {
            logger.debug("can't reach the fetch contexts of [{}], the node left the cluster", node);
        }
        return found;
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
            return transportService.getThreadPool().scheduleWithFixedDelay(refresh, interval, generic());
        } catch (EsRejectedExecutionException e) {
            logger.debug("can't refresh fetch contexts", e);
            return null;
        }
    }

    /**
     * Keeps every context of {@code request} open for at least the keep-alive of the request, from now, and at most for
     * {@code search.max_keep_alive} of this node, as the contexts opened. Skips a context that {@link #fetchContext}
     * doesn't find: the fetch that needs it reports it. The response is the same either way, so it tells nobody which
     * contexts exist.
     */
    void serveKeepAlive(FetchContextKeepAliveRequest request) {
        assert Transports.assertNotTransportThread("dropping a reference to a fetch context can close its reader");
        IndicesAccessControl accessControl = ShardAccess.of(transportService.getThreadPool().getThreadContext());
        long keepAlive = Math.min(request.keepAlive().millis(), searchService.getMaxKeepAliveInMillis());
        for (ShardSearchContextId id : request.contextIds()) {
            try {
                fetchContext(id, request, accessControl, KEEP_ALIVE_ACTION_NAME).markAsUsed(keepAlive).close();
            } catch (SearchContextMissingException | ElasticsearchSecurityException e) {
                logger.debug("can't keep fetch context [{}] alive: {}", id, e.getMessage());
            }
        }
    }

    /**
     * Frees every context of {@code request}. Skips a context that {@link #fetchContext} doesn't find, which the reaper frees
     * once its keep-alive passed if it exists. The response is the same either way, so it tells nobody which contexts exist.
     */
    void serveFree(FetchContextFreeRequest request) {
        assert Transports.assertNotTransportThread("freeing a fetch context can close its reader");
        IndicesAccessControl accessControl = ShardAccess.of(transportService.getThreadPool().getThreadContext());
        for (ShardSearchContextId id : request.contextIds()) {
            try {
                searchService.freeReaderContext(fetchContext(id, request, accessControl, FREE_ACTION_NAME).id());
            } catch (SearchContextMissingException | ElasticsearchSecurityException e) {
                logger.debug("can't free fetch context [{}]: {}", id, e.getMessage());
            }
        }
    }

    /**
     * The fetch context {@code id} that {@code request} may use.
     *
     * @throws SearchContextMissingException when the context is gone, has another searcher id, or is a scroll or a point in
     *                                       time, which isn't a context of the fetch phase
     * @throws ElasticsearchSecurityException when the request may not read the index of the context
     */
    private ReaderContext fetchContext(
        ShardSearchContextId id,
        TransportRequest request,
        IndicesAccessControl accessControl,
        String action
    ) {
        ReaderContext context = searchService.findReaderContext(id, request, null);
        // the lookup ignores the searcher id
        if (context.id().equals(id) == false || context.singleSession() == false) {
            throw new SearchContextMissingException(id);
        }
        ShardAccess.check(accessControl, context.indexShard().shardId(), action);
        return context;
    }

    SearchService searchService() {
        return searchService;
    }

    Executor generic() {
        return transportService.getThreadPool().generic();
    }
}
