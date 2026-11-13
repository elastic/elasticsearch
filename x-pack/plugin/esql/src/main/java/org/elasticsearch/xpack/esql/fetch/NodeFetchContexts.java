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
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * The reader contexts that one data node request opens for the fetch phase, one per shard.
 * <p>
 * Each one is a single-session reader context of {@link SearchService}, like the contexts the query phase of
 * {@code _search} opens, so the reaper, index removal and node shutdown free it if nobody else does. The request holds its
 * own reference on each context, so none expires while the drivers run or while the coordinator drains the exchange.
 * <p>
 * When the request responds, {@link #handOffContributing} hands the contexts whose rows survived the node cut to the
 * coordinator. {@link #close} frees the rest, which is every context when the request failed or was cancelled.
 * <p>
 * Both drop the request's references on the generic pool. Dropping the last reference to a reader can unmap files and
 * block, and a request can end on a transport thread, for example when the coordinator drained the exchange.
 */
public final class NodeFetchContexts implements DocOriginResolver, Releasable {
    private static final Logger logger = LogManager.getLogger(NodeFetchContexts.class);

    /**
     * One context this request opened.
     */
    private static final class OpenContext {
        private final ReaderContext readerContext;
        private final DocOrigin origin;
        /**
         * The request's reference on the context.
         */
        private final Releasable lease;
        /**
         * Some rows read through the context survived the node cut. Set by the drivers.
         */
        private volatile boolean contributing;

        OpenContext(ReaderContext readerContext, DocOrigin origin, Releasable lease) {
            this.readerContext = readerContext;
            this.origin = origin;
            this.lease = lease;
        }
    }

    private final SearchService searchService;
    private final Executor generic;
    /**
     * The creator of the contexts, logged when they open and close.
     */
    @Nullable
    private final Task task;
    /**
     * The keep-alive each context gets when the coordinator takes it over.
     */
    private final TimeValue keepAlive;
    /**
     * The contexts this request still owns.
     */
    private final Map<ShardSearchContextId, OpenContext> contexts = new ConcurrentHashMap<>();
    /**
     * Guards against a context that opens after {@link #close}, which nobody would free.
     */
    private boolean closed;

    /**
     * @param keepAlive the keep-alive each context gets when the coordinator takes it over. A context never gets less than
     *                  {@code search.default_keep_alive}, the keep-alive it opens with.
     */
    public NodeFetchContexts(SearchService searchService, Executor generic, @Nullable Task task, TimeValue keepAlive) {
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.searchService = searchService;
        this.generic = generic;
        this.task = task;
        this.keepAlive = keepAlive;
    }

    /**
     * Opens a single-session reader context for the shard of {@code shardRequest}, and returns the search context the
     * query phase reads it with. Every resource taken so far is released when it fails.
     */
    public SearchContext open(ShardSearchRequest shardRequest) throws IOException {
        ReaderContext readerContext = null;
        SearchContext querySearchContext = null;
        Releasable lease = null;
        boolean success = false;
        try {
            readerContext = searchService.createOrGetReaderContext(shardRequest, task);
            querySearchContext = searchService.createSearchContext(readerContext, shardRequest, SearchService.NO_TIMEOUT);
            // taken last: the query search context already holds a reference, so the context can't be gone.
            // -1 leaves the keep-alive as it is, the hand-off sets it.
            lease = readerContext.markAsUsed(-1L);
            register(new OpenContext(readerContext, origin(querySearchContext), lease));
            success = true;
            return querySearchContext;
        } finally {
            if (success == false) {
                Releasables.closeWhileHandlingException(lease, querySearchContext);
                if (readerContext != null) {
                    searchService.freeReaderContext(readerContext.id());
                }
            }
        }
    }

    private synchronized void register(OpenContext open) {
        if (closed) {
            throw new IllegalStateException("opened a fetch context after the request ended");
        }
        contexts.put(open.readerContext.id(), open);
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
        open.contributing = true;
        return open.origin;
    }

    /**
     * Hands the contexts whose rows survived the node cut to the coordinator and returns their ids, for the response. Each
     * gets the keep-alive of the request, which starts once the request dropped its reference. From then on the
     * coordinator frees them, or the reaper once their keep-alive passed.
     */
    public List<ShardSearchContextId> handOffContributing() {
        List<OpenContext> handedOff = new ArrayList<>();
        List<ShardSearchContextId> ids = new ArrayList<>();
        for (OpenContext context : contexts.values()) {
            if (context.contributing && contexts.remove(context.readerContext.id(), context)) {
                // the request still holds its own reference, so this one is never the last
                context.readerContext.markAsUsed(keepAlive.millis()).close();
                handedOff.add(context);
                ids.add(context.readerContext.id());
            }
        }
        if (handedOff.isEmpty() == false) {
            onGeneric(() -> handedOff.forEach(NodeFetchContexts::dropLease));
        }
        return ids;
    }

    /**
     * Frees the contexts this request still owns: those whose rows didn't survive the node cut, or every context when the
     * request failed or was cancelled. Called last, once the request closed the search contexts of its query phase.
     */
    @Override
    public void close() {
        List<OpenContext> owned;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            owned = new ArrayList<>(contexts.values());
            contexts.clear();
        }
        if (owned.isEmpty() == false) {
            onGeneric(() -> owned.forEach(this::free));
        }
    }

    private void free(OpenContext context) {
        assert Transports.assertNotTransportThread("freeing a fetch context can close its reader");
        try {
            boolean found = searchService.freeReaderContext(context.readerContext.id());
            logger.debug(
                "freed fetch context [{}] of shard [{}]{}",
                context.readerContext.id(),
                context.origin.shardId(),
                found ? "" : ", it was already gone"
            );
        } finally {
            dropLease(context);
        }
    }

    private static void dropLease(OpenContext context) {
        assert Transports.assertNotTransportThread("dropping the last reference to a fetch context closes its reader");
        context.lease.close();
    }

    private void onGeneric(Runnable run) {
        generic.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                run.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("failed to release fetch contexts", e);
            }
        });
    }
}
