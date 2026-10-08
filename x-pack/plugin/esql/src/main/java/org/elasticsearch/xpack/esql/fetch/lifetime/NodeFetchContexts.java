/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The reader contexts that one data node request opens for the fetch phase, one per shard.
 * <p>
 * Each context is registered in {@link SearchService}, so the reaper, index removal and node shutdown free it if nobody
 * else does. The request owns its contexts until it has sent its response. Then it frees the ones whose rows didn't
 * survive the node cut, and the coordinator owns the rest.
 * <p>
 * Until the request ends it holds its own reference on each context, so none expires while the drivers run or while the
 * coordinator drains the exchange. It drops those references last, in {@link #close}, on the generic pool, after the search
 * contexts of the query phase closed. Dropping the last reference to a reader can unmap files and block, and a request can
 * end on a transport thread, for example when it is cancelled.
 */
public final class NodeFetchContexts implements DocRefOriginResolver, Releasable {
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
    private record OpenContext(ReaderContext readerContext, DocRefOrigin origin, Releasable nodeLease, Flags flags) {}

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
    private final TimeValue keepAlive;
    /**
     * The creator of the contexts. Dropped when the request stops opening contexts, because a context can outlive the
     * request by its keep-alive and holds this object through its marker.
     */
    @Nullable
    private volatile Task task;
    /**
     * The contexts that are still open. A context leaves when it closes.
     */
    private final Map<ShardSearchContextId, OpenContext> contexts = new ConcurrentHashMap<>();
    private State state = State.OPEN;
    private boolean released;

    NodeFetchContexts(FetchContextService service, TimeValue keepAlive, @Nullable Task task) {
        this.service = service;
        this.keepAlive = keepAlive;
        this.task = task;
    }

    /**
     * Opens a registered reader context for the shard of {@code shardRequest}, bound to the current user, and returns the
     * search context the query phase reads it with. Every resource taken so far is released when it fails.
     *
     * @throws org.elasticsearch.ElasticsearchStatusException with status 429 when the node holds too many fetch contexts
     * @throws IllegalArgumentException when the keep-alive is longer than this node allows
     * @throws TaskCancelledException when the request failed or was cancelled
     */
    public SearchContext open(ShardSearchRequest shardRequest) throws IOException {
        // checked first, so a cancelled request doesn't acquire a searcher for every remaining shard
        ensureOpen();
        SearchService searchService = service.searchService();
        if (keepAlive.millis() > searchService.getMaxKeepAliveInMillis()) {
            throw new IllegalArgumentException(
                "the coordinator asks to keep fetch contexts for ["
                    + keepAlive
                    + "], set by ["
                    + FetchContextService.CONTEXT_KEEP_ALIVE.getKey()
                    + "], but ["
                    + SearchService.MAX_KEEPALIVE_SETTING.getKey()
                    + "] allows at most ["
                    + TimeValue.timeValueMillis(searchService.getMaxKeepAliveInMillis())
                    + "] on this node"
            );
        }
        service.reserve();
        FetchContextListener.OpenMarker marker = new FetchContextListener.OpenMarker(this);
        ReaderContext readerContext = null;
        SearchContext querySearchContext = null;
        Releasable nodeLease = null;
        boolean success = false;
        try {
            ThreadContext threadContext = service.threadContext();
            try (var ignored = threadContext.newStoredContextPreservingResponseHeaders()) {
                // the listener runs inside the open, on this thread, and reads the marker
                threadContext.putTransient(FetchContextListener.OPEN_MARKER, marker);
                readerContext = searchService.openOwnedReaderContext(shardRequest, keepAlive, task);
            }
            if (marker.bound() == false) {
                // without its owner and its marker, the context would be open to anyone who guesses its id
                throw new IllegalStateException(
                    "no fetch context listener bound reader context [" + readerContext.id() + "] of shard [" + shardRequest.shardId() + "]"
                );
            }
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
                if (marker.bound() == false) {
                    // no listener follows this context, so its close releases nothing. Once a listener bound it, the
                    // listener releases it when it closes, also when the open itself failed and closed it.
                    service.release();
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

    private static DocRefOrigin origin(SearchContext searchContext) {
        SearchShardTarget target = searchContext.shardTarget();
        String clusterAlias = target.getClusterAlias() == null ? RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY : target.getClusterAlias();
        return new DocRefOrigin(clusterAlias, target.getNodeId(), target.getShardId(), searchContext.readerContext().id());
    }

    /**
     * Marks the context of {@code searchContext} as contributing, because some of its rows are leaving the node.
     */
    @Override
    public DocRefOrigin originOf(SearchContext searchContext) {
        ShardSearchContextId id = searchContext.readerContext().id();
        OpenContext open = contexts.get(id);
        if (open == null) {
            throw new IllegalStateException("[" + id + "] isn't an open fetch context of this request");
        }
        open.flags().contributing = true;
        return open.origin();
    }

    /**
     * The contexts whose rows survived the node cut and that are still open, for the response. Changes no reference, so
     * it can run on the thread that sends the response.
     */
    public List<OpenContextInfo> listOpenContributing() {
        List<OpenContextInfo> open = new ArrayList<>();
        for (OpenContext context : contexts.values()) {
            if (context.flags().contributing) {
                open.add(new OpenContextInfo(context.origin().shardId(), context.readerContext().id()));
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
                context.nodeLease().close();
            }
        });
    }

    /**
     * Frees one context of this node, also one that hasn't finished opening yet.
     */
    void free(ShardSearchContextId id, String reason) {
        onGeneric(() -> {
            boolean found = service.searchService().freeReaderContext(id);
            logger.debug("freed fetch context [{}] because {}{}", id, reason, found ? "" : ", it was already gone");
        });
    }

    /**
     * Called by the listener when a context of this request closed, on any path, also one that never finished opening.
     */
    void onReaderContextClosed(ReaderContext readerContext) {
        service.release();
        contexts.remove(readerContext.id());
    }

    private void freeOnce(OpenContext context, String reason) {
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
