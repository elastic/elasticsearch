/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The reader contexts that one data node request opens for the fetch phase, one per shard.
 * <p>
 * Each one is a single-session reader context of {@link SearchService}, like the contexts the query phase of
 * {@code _search} opens, so the reaper, index removal and node shutdown free it if nobody else does. The request owns its
 * contexts until it has sent its response. Then it frees the ones whose rows didn't survive the node cut, and the
 * coordinator owns the rest.
 * <p>
 * Until the request ends it holds its own reference on each context, so none expires while the drivers run or while the
 * coordinator drains the exchange. It drops those references last, in {@link #close}, on the generic pool, after the search
 * contexts of the query phase closed. Dropping the last reference to a reader can unmap files and block, and a request can
 * end on a transport thread, for example when it is cancelled.
 */
public final class NodeFetchContexts implements DocOriginResolver, Releasable {
    private static final Logger logger = LogManager.getLogger(NodeFetchContexts.class);

    private enum State {
        /** Opening contexts and running the query phase. */
        OPEN,
        /** The response listed the contributing contexts. The coordinator owns them now. */
        RESPONDED,
        /** The request failed or was cancelled. Every context is freed, and no new one may open. */
        CLOSED
    }

    /**
     * One context this request opened.
     *
     * @param nodeLease the request's reference on the context, dropped by {@link #close}
     */
    private record OpenContext(ReaderContext readerContext, DocOrigin origin, Releasable nodeLease, Flags flags) {}

    /**
     * What happened to an {@link OpenContext} since it opened.
     */
    private static final class Flags {
        /** Some rows read through the context survived the node cut. Set by the drivers. */
        private volatile boolean contributing;
        /** This request freed the context. Frees from several paths race, and only the first one frees. */
        private final AtomicBoolean freed = new AtomicBoolean();
    }

    private final FetchContextService service;
    /**
     * The creator of the contexts, logged when they open and close. Dropped when the request stops opening contexts.
     */
    @Nullable
    private volatile Task task;
    /**
     * The contexts this request opened. The request holds a reference on each until it ends, so none closes before.
     */
    private final Map<ShardSearchContextId, OpenContext> contexts = new ConcurrentHashMap<>();
    private State state = State.OPEN;
    private boolean released;

    NodeFetchContexts(FetchContextService service, @Nullable Task task) {
        this.service = service;
        this.task = task;
    }

    /**
     * Opens a single-session reader context for the shard of {@code shardRequest}, and returns the search context the
     * query phase reads it with. Every resource taken so far is released when it fails.
     *
     * @throws TaskCancelledException when the request failed or was cancelled
     */
    public SearchContext open(ShardSearchRequest shardRequest) throws IOException {
        // checked first, so a cancelled request doesn't acquire a searcher for every remaining shard
        ensureOpen();
        SearchService searchService = service.searchService();
        ReaderContext readerContext = null;
        SearchContext querySearchContext = null;
        Releasable nodeLease = null;
        boolean success = false;
        try {
            readerContext = searchService.createOrGetReaderContext(shardRequest, task);
            querySearchContext = searchService.createSearchContext(readerContext, shardRequest, SearchService.NO_TIMEOUT);
            // taken last: the query search context already holds a reference, so the context can't be gone
            nodeLease = readerContext.markAsUsed(-1L);
            register(new OpenContext(readerContext, origin(querySearchContext), nodeLease, new Flags()));
            success = true;
            return querySearchContext;
        } finally {
            if (success == false) {
                Releasables.closeWhileHandlingException(nodeLease, querySearchContext);
                if (readerContext != null) {
                    searchService.freeReaderContext(readerContext.id());
                }
            }
        }
    }

    private synchronized void ensureOpen() {
        switch (state) {
            case OPEN -> {
            }
            case RESPONDED -> throw new IllegalStateException("opened a fetch context after the response");
            case CLOSED -> throw new TaskCancelledException("the fetch contexts of this request are closed");
        }
    }

    private synchronized void register(OpenContext open) {
        ensureOpen();
        contexts.put(open.readerContext().id(), open);
    }

    private static DocOrigin origin(SearchContext searchContext) {
        SearchShardTarget target = searchContext.shardTarget();
        String clusterAlias = target.getClusterAlias() == null ? RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY : target.getClusterAlias();
        return new DocOrigin(clusterAlias, target.getNodeId(), target.getShardId(), searchContext.readerContext().id());
    }

    /**
     * Marks the context of {@code searchContext} as contributing, because some of its rows are leaving the node.
     */
    @Override
    public DocOrigin originOf(SearchContext searchContext) {
        ShardSearchContextId id = searchContext.readerContext().id();
        OpenContext open = contexts.get(id);
        if (open == null) {
            throw new IllegalStateException("[" + id + "] isn't an open fetch context of this request");
        }
        open.flags().contributing = true;
        return open.origin();
    }

    /**
     * The contexts whose rows survived the node cut, for the response. Changes no reference, so it can run on the thread
     * that sends the response.
     */
    public List<ShardSearchContextId> listOpenContributing() {
        List<ShardSearchContextId> open = new ArrayList<>();
        for (OpenContext context : contexts.values()) {
            if (context.flags().contributing) {
                open.add(context.readerContext().id());
            }
        }
        return open;
    }

    /**
     * Called once the response that lists the contributing contexts is sent. The coordinator owns those from now on.
     */
    public void responded() {
        synchronized (this) {
            if (state == State.OPEN) {
                state = State.RESPONDED;
            }
        }
        task = null;
    }

    /**
     * Frees every context, for example because the request failed or was cancelled. A context that opens afterwards is
     * freed at once and fails its open. The readers stay open until {@link #close} drops the request's references.
     */
    public void freeAll(String reason) {
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
        }
        task = null;
        onGeneric(() -> {
            for (OpenContext context : contexts.values()) {
                freeOnce(context, reason);
            }
        });
    }

    /**
     * Called last, once the request closed the search contexts of its query phase. After a response it frees the contexts
     * whose rows didn't survive the node cut. Then it drops the request's reference on every context, which starts the
     * keep-alive of the contexts the coordinator owns. Everything runs on the generic pool.
     */
    @Override
    public void close() {
        State endState;
        synchronized (this) {
            if (released) {
                return;
            }
            released = true;
            endState = state;
            if (state == State.OPEN) {
                // the request ended without a response or a failure, so nobody else frees its contexts
                state = State.CLOSED;
            }
        }
        task = null;
        onGeneric(() -> {
            for (OpenContext context : contexts.values()) {
                switch (endState) {
                    case OPEN -> freeOnce(context, "the request ended without a response");
                    case RESPONDED -> {
                        if (context.flags().contributing == false) {
                            freeOnce(context, "no rows survived the node cut");
                        }
                    }
                    case CLOSED -> {
                        // freeAll freed them already
                    }
                }
                assert Transports.assertNotTransportThread("dropping the last reference to a fetch context closes its reader");
                context.nodeLease().close();
            }
        });
    }

    private void freeOnce(OpenContext context, String reason) {
        assert Transports.assertNotTransportThread("freeing a fetch context can close its reader");
        if (context.flags().freed.compareAndSet(false, true)) {
            boolean found = service.searchService().freeReaderContext(context.readerContext().id());
            logger.debug(
                "freed fetch context [{}] of shard [{}] because {}{}",
                context.readerContext().id(),
                context.origin().shardId(),
                reason,
                found ? "" : ", it was already gone"
            );
        }
    }

    private void onGeneric(Runnable run) {
        service.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                run.run();
            }

            @Override
            public void onFailure(Exception e) {
                // the reaper frees what this left behind once the keep-alive passes
                logger.warn("failed to free fetch contexts", e);
            }
        });
    }
}
