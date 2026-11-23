/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
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
import org.elasticsearch.search.scheduling.QueryScheduler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
 * <p>
 * Rows that the sink of the request discarded never left, so they keep no context. The sink discards the pages it still
 * buffers when the query is stopped, cancelled or fails, and the coordinator never hears of the contexts those pages name.
 * A context is freed once the sink discarded every block of documents with its rows, also when the discard comes after the
 * request ended, as on cancellation, where the sink fails before it discards. A context stays kept while any block with its
 * rows may reach the coordinator.
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
         * The blocks of documents with rows of the context that went to the sink, less the ones the sink discarded. The
         * drivers count up, the sink counts down.
         */
        private final AtomicInteger leavingBlocks = new AtomicInteger();

        OpenContext(ReaderContext readerContext, DocOrigin origin, Releasable lease) {
            this.readerContext = readerContext;
            this.origin = origin;
            this.lease = lease;
        }
    }

    private final SearchService searchService;
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
     * Guards against a context that opens after {@link #close}, which nobody would release. Once set, a discard frees the
     * contexts whose last block it discards.
     */
    private volatile boolean closed;

    /**
     * @param keepAlive the keep-alive of each context the request keeps. A context never gets less than
     *                  {@code search.default_keep_alive}, the keep-alive it opens with.
     */
    public NodeFetchContexts(SearchService searchService, @Nullable Task task, TimeValue keepAlive) {
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.searchService = searchService;
        this.task = task;
        this.keepAlive = keepAlive;
    }

    /**
     * Opens a single-session reader context for the shard of {@code shardRequest}, and returns the search context the
     * query phase reads it with. That context charges the breaker of {@code part}, if given. On failure, everything
     * taken so far is released, {@code part} included.
     */
    public SearchContext open(ShardSearchRequest shardRequest, @Nullable QueryScheduler.QueryPart part) throws IOException {
        ReaderContext readerContext = null;
        SearchContext querySearchContext = null;
        Releasable lease = null;
        boolean success = false;
        try {
            readerContext = searchService.createOrGetReaderContext(shardRequest, task);
            querySearchContext = searchService.createSearchContext(readerContext, shardRequest, SearchService.NO_TIMEOUT, part);
            // the search context closes the part from here on
            part = null;
            // taken last: the query search context already holds a reference, so the context can't be gone.
            // -1 leaves the keep-alive as it is, close sets it.
            lease = readerContext.markAsUsed(-1L);
            register(new OpenContext(readerContext, origin(querySearchContext), lease));
            success = true;
            return querySearchContext;
        } finally {
            if (success == false) {
                Releasables.closeWhileHandlingException(lease, querySearchContext, part);
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
     * The origin of the rows read through {@code searchContext}, for a block of documents that leaves the node. Asked once
     * for each such block ({@link DocVector#detach}), so the request keeps the context unless the sink discards every one of
     * them ({@link #discarded}).
     */
    public DocOrigin originOf(SearchContext searchContext) {
        ShardSearchContextId id = searchContext.readerContext().id();
        OpenContext open = contexts.get(id);
        if (open == null) {
            throw new IllegalStateException("[" + id + "] isn't an open fetch context of this request");
        }
        // #discarded relies on it: the request ends once all of its drivers finished
        assert closed == false : "[" + id + "] a block of documents went to the sink after the request ended";
        open.leavingBlocks.incrementAndGet();
        return open.origin;
    }

    /**
     * The sink discarded {@code page} instead of sending it, so its blocks of documents no longer keep their contexts. Once
     * the request ended no block goes to the sink anymore, so a context whose last block this discards is freed at once.
     * The sink calls it, so it never throws.
     */
    public void discarded(Page page) {
        List<OpenContext> unsent = new ArrayList<>();
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc && doc.asVector().origins() != null) {
                DocOrigins origins = doc.asVector().origins();
                for (int o = 0; o < origins.size(); o++) {
                    OpenContext open = contexts.get(origins.get(o).contextId());
                    if (open == null) {
                        assert false : "[" + origins.get(o).contextId() + "] isn't a fetch context of this request";
                        continue;
                    }
                    int left = open.leavingBlocks.decrementAndGet();
                    assert left >= 0 : "[" + open.readerContext.id() + "] discarded more blocks than went to the sink";
                    // Read closed after the count: a request that ends in between sees the count at zero, and frees the
                    // context itself. Then read the count again: a driver may have sent another block of the context before
                    // the request ended, and the request kept the context for it. Once closed, the count only goes down.
                    if (left == 0 && closed && open.leavingBlocks.get() == 0) {
                        unsent.add(open);
                    }
                }
            }
        }
        if (unsent.isEmpty() == false) {
            freeUnsent(unsent);
        }
    }

    /**
     * Frees the contexts that the request kept, but whose blocks the sink then discarded. Like {@link #close}, it runs on the
     * free context executor.
     */
    private void freeUnsent(List<OpenContext> unsent) {
        searchService.getFreeContextExecutor().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                for (OpenContext context : unsent) {
                    try {
                        boolean found = searchService.freeReaderContext(context.readerContext.id());
                        logger.debug(
                            "freed fetch context [{}] of shard [{}], the sink discarded all of its rows{}",
                            context.readerContext.id(),
                            context.origin.shardId(),
                            found ? "" : ", it was already gone"
                        );
                    } catch (Exception e) {
                        // the others still need their free
                        logger.warn(() -> "failed to free fetch context [" + context.readerContext.id() + "]", e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("failed to free fetch contexts", e);
            }
        });
    }

    /**
     * Keeps the contexts some rows that left the node read, with the keep-alive of the request, and frees the others. Called
     * last, once all drivers of the request finished and the request closed the search contexts of its query phase. So no
     * block of documents goes to the sink after it, but the sink can still discard pages, see {@link #discarded}.
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
            // the map stays, so a later discard finds the contexts it frees
            opened = new ArrayList<>(contexts.values());
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
            if (context.leavingBlocks.get() > 0) {
                // the request still holds its own reference, so this one is never the last
                context.readerContext.markAsUsed(keepAlive.millis()).close();
            } else {
                boolean found = searchService.freeReaderContext(context.readerContext.id());
                logger.debug(
                    "freed fetch context [{}] of shard [{}], none of its rows left the node{}",
                    context.readerContext.id(),
                    context.origin.shardId(),
                    found ? "" : ", it was already gone"
                );
            }
        }
    }
}
