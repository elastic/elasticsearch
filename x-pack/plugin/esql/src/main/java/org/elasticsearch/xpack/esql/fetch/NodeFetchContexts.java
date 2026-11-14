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

/**
 * The reader contexts that one data node request opens for the fetch phase, one per shard.
 * <p>
 * Each one is a single-session reader context of {@link SearchService}, like the contexts the query phase of
 * {@code _search} opens, so the reaper, index removal and node shutdown free it if nobody else does. The request holds its
 * own reference on each context, so none expires while the drivers run or while the coordinator drains the exchange.
 * <p>
 * When the request ends, {@link #close} keeps the contexts some rows that left the node read, and frees the others. The
 * coordinator finds the kept ones in the origins of those rows, and frees them once it fetched, or the reaper does once their
 * keep-alive passed. A failed request keeps them too: with partial results, the rows that left the node before the failure
 * can still need their fetch.
 */
public final class NodeFetchContexts implements Releasable {
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
         * Some rows read through the context left the node. Set by the drivers.
         */
        private volatile boolean contributing;

        OpenContext(ReaderContext readerContext, DocOrigin origin, Releasable lease) {
            this.readerContext = readerContext;
            this.origin = origin;
            this.lease = lease;
        }
    }

    private final SearchService searchService;
    private final OpenFetchContexts openContexts;
    /**
     * The creator of the contexts, logged when they open and close.
     */
    @Nullable
    private final Task task;
    /**
     * The keep-alive of each context the request keeps.
     */
    private final TimeValue keepAlive;
    private final Map<ShardSearchContextId, OpenContext> contexts = new ConcurrentHashMap<>();
    /**
     * Guards against a context that opens after {@link #close}, which nobody would release.
     */
    private boolean closed;

    /**
     * @param openContexts counts the contexts the request opens until their readers close
     * @param keepAlive    the keep-alive of each context the request keeps. A context never gets less than
     *                     {@code search.default_keep_alive}, the keep-alive it opens with.
     */
    public NodeFetchContexts(SearchService searchService, OpenFetchContexts openContexts, @Nullable Task task, TimeValue keepAlive) {
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.searchService = searchService;
        this.openContexts = openContexts;
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
            openContexts.opened(readerContext);
            querySearchContext = searchService.createSearchContext(readerContext, shardRequest, SearchService.NO_TIMEOUT);
            // taken last: the query search context already holds a reference, so the context can't be gone.
            // -1 leaves the keep-alive as it is, close sets it.
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
     * The origin of the rows read through {@code searchContext}, for the rows that leave the node. Asking marks the context
     * as one the request keeps.
     */
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
     * Keeps the contexts some rows that left the node read, with the keep-alive of the request, and frees the others. Called
     * last, once the request closed the search contexts of its query phase.
     * <p>
     * Runs on the free context executor of {@link SearchService}, like the frees of {@code _search}. Dropping the last
     * reference to a reader can unmap files and block, and a request can end on a transport thread, for example when the
     * coordinator drained the exchange.
     */
    @Override
    public void close() {
        List<OpenContext> opened;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            opened = new ArrayList<>(contexts.values());
            contexts.clear();
        }
        if (opened.isEmpty()) {
            return;
        }
        searchService.getFreeContextExecutor().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                assert Transports.assertNotTransportThread("releasing a fetch context can close its reader");
                for (OpenContext context : opened) {
                    try {
                        release(context);
                    } catch (Exception e) {
                        // the others still need their release
                        logger.warn(() -> "failed to release fetch context [" + context.readerContext.id() + "]", e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("failed to release fetch contexts", e);
            }
        });
    }

    private void release(OpenContext context) {
        try (context.lease) {
            if (context.contributing) {
                // the request still holds its own reference, so this one is never the last
                context.readerContext.markAsUsed(keepAlive.millis()).close();
            } else {
                boolean found = searchService.freeReaderContext(context.readerContext.id());
                logger.debug(
                    "freed fetch context [{}] of shard [{}], no row read it{}",
                    context.readerContext.id(),
                    context.origin.shardId(),
                    found ? "" : ", it was already gone"
                );
            }
        }
    }
}
