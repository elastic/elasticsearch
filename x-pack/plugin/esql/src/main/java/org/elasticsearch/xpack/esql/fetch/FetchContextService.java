/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;

import java.util.concurrent.Executor;

/**
 * The reader contexts that this node keeps open for the fetch phase of ES|QL queries, as a data node.
 * <p>
 * A data node request of the query phase opens a single-session reader context per shard, like the query phase of
 * {@code _search}, and keeps the ones whose rows leave the node, see {@link NodeFetchContexts}. Their coordinator then
 * refreshes their keep-alive and frees them with the requests this service serves. The coordinator side is
 * {@link FetchContextLeases}.
 */
public final class FetchContextService {
    /**
     * Data nodes keep reader contexts for the fetch phase, and serve the keep-alive and free requests of their
     * coordinators.
     */
    public static final TransportVersion ESQL_FETCH_CONTEXTS = TransportVersion.fromName("esql_fetch_phase_plan");

    /**
     * Refreshes the keep-alive of fetch contexts. Its requests carry the index expressions of the query, so they are
     * authorized like the query's other index actions.
     */
    public static final String KEEP_ALIVE_ACTION_NAME = EsqlQueryAction.NAME + "/fetch/keep_alive";

    /**
     * Frees fetch contexts. Its requests carry the index expressions of the query, so they are authorized like the query's
     * other index actions.
     */
    public static final String FREE_ACTION_NAME = EsqlQueryAction.NAME + "/fetch/free";

    private static final Logger logger = LogManager.getLogger(FetchContextService.class);

    private final SearchService searchService;
    private final TransportService transportService;
    private final OpenFetchContexts openContexts;

    /**
     * @param openContexts counts the contexts this node opens
     */
    public FetchContextService(SearchService searchService, TransportService transportService, OpenFetchContexts openContexts) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.openContexts = openContexts;
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
     * Lets a gateway node forward the requests of a coordinator of another cluster that isn't connected to the node they go
     * to. The free request isn't a child of a task, so neither is its forwarded copy.
     */
    public void registerProxyActions(NamedWriteableRegistry namedWriteableRegistry) {
        TransportActionProxy.registerProxyAction(
            transportService,
            KEEP_ALIVE_ACTION_NAME,
            false,
            in -> ActionResponse.Empty.INSTANCE,
            namedWriteableRegistry
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            FREE_ACTION_NAME,
            false,
            in -> ActionResponse.Empty.INSTANCE,
            namedWriteableRegistry
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

    OpenFetchContexts openContexts() {
        return openContexts;
    }

    Executor generic() {
        return transportService.getThreadPool().generic();
    }
}
