/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The fetch contexts that the data nodes keep for one execution of a query. A data node keeps the contexts that the
 * origins of a page that left it name, and the coordinator records them from the pages that reach it ({@link #track}). The
 * tracker frees them the way {@code _search} frees the contexts of its query phase:
 * <ol>
 *     <li>once the cut ended, the contexts that no row of the cut reads ({@link #cutEnded}),</li>
 *     <li>a node frees a context itself with the last fetch request that reads it, and the tracker forgets it then
 *     ({@link #freedByTheirNode}),</li>
 *     <li>once the fetch ended, or the execution failed or was cancelled, every context left ({@link #close}).</li>
 * </ol>
 * A context that arrives after the tracker stopped holding it is freed at once.
 * <p>
 * It frees each context with the free action of {@code _search}, one request per context, as the user who ran the query,
 * also when an administrator cancels it. It doesn't wait for the requests. A request that fails, or a node that left,
 * leaves the context to the reaper of its node, which frees it once its keep-alive passed. So do the contexts of pages that
 * never reach the coordinator.
 * <p>
 * The fetch phase reads the local cluster only, so the tracker frees the contexts of local nodes only. It is thread-safe:
 * the exchange and the fetch operator call it from their own threads.
 */
public final class FetchContextTracker implements Releasable {
    private static final Logger logger = LogManager.getLogger(FetchContextTracker.class);

    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final Supplier<ThreadContext.StoredContext> ownerContext;
    private final Set<DocOrigin> held = new HashSet<>();
    /**
     * The contexts that the tracker or their node freed. Pages of the same context can keep arriving, and one free is
     * enough.
     */
    private final Set<DocOrigin> freed = new HashSet<>();
    /**
     * The contexts the rows of the cut read, once the cut ended.
     */
    @Nullable
    private Set<ShardSearchContextId> read;
    private boolean closed;

    /**
     * Captures the thread context of the caller, the user who runs the query, to free the contexts as that user.
     */
    public FetchContextTracker(SearchTransportService searchTransportService, ClusterService clusterService) {
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.ownerContext = searchTransportService.transportService().getThreadPool().getThreadContext().newRestorableContext(false);
    }

    /**
     * Records the contexts that the origins of the documents of {@code page} name, whether or not a row of the page reads
     * them. Called for every page that reaches the coordinator, before the exchange buffers it, so it never throws.
     */
    public void track(Page page) {
        List<DocOrigin> toFree = new ArrayList<>();
        synchronized (this) {
            for (int b = 0; b < page.getBlockCount(); b++) {
                if (page.getBlock(b) instanceof DocBlock doc && doc.asVector().origins() != null) {
                    DocOrigins origins = doc.asVector().origins();
                    for (int o = 0; o < origins.size(); o++) {
                        DocOrigin origin = origins.get(o);
                        if (freed.contains(origin)) {
                            continue;
                        }
                        if (closed == false && reads(origin.contextId())) {
                            held.add(origin);
                        } else {
                            freed.add(origin);
                            // the execution ended, or stopped reading the context, before its page arrived
                            toFree.add(origin);
                        }
                    }
                }
            }
        }
        free(toFree);
    }

    /**
     * The cut ended, and its rows read only the contexts in {@code read}. Frees the others, before the fetch loads the
     * documents of the rows.
     */
    public void cutEnded(Set<ShardSearchContextId> read) {
        List<DocOrigin> unread = new ArrayList<>();
        synchronized (this) {
            // a second cut would free the contexts that the rows of the first one read
            assert this.read == null : "an execution has one cut";
            this.read = Set.copyOf(read);
            Iterator<DocOrigin> origins = held.iterator();
            while (origins.hasNext()) {
                DocOrigin origin = origins.next();
                if (reads(origin.contextId()) == false) {
                    unread.add(origin);
                    origins.remove();
                }
            }
            freed.addAll(unread);
        }
        free(unread);
    }

    /**
     * Their nodes freed the contexts {@code ids} themselves, once they answered the fetch request that read them for the last
     * time. The tracker forgets them, so it doesn't free them again.
     */
    public void freedByTheirNode(Collection<ShardSearchContextId> ids) {
        if (ids.isEmpty()) {
            return;
        }
        synchronized (this) {
            Iterator<DocOrigin> origins = held.iterator();
            while (origins.hasNext()) {
                DocOrigin origin = origins.next();
                if (ids.contains(origin.contextId())) {
                    freed.add(origin);
                    origins.remove();
                }
            }
        }
    }

    /**
     * Frees every context left, once the fetch ended, or the execution failed or was cancelled. Closing twice does nothing.
     */
    @Override
    public void close() {
        List<DocOrigin> left;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            left = new ArrayList<>(held);
            freed.addAll(held);
            held.clear();
        }
        free(left);
    }

    private boolean reads(ShardSearchContextId id) {
        return read == null || read.contains(id);
    }

    /**
     * Frees {@code origins} as the user who ran the query. Called without the lock. Never throws.
     */
    private void free(List<DocOrigin> origins) {
        if (origins.isEmpty()) {
            return;
        }
        try (ThreadContext.StoredContext ignored = ownerContext.get()) {
            DiscoveryNodes nodes = clusterService.state().nodes();
            for (DocOrigin origin : origins) {
                free(nodes, origin);
            }
        }
    }

    private void free(DiscoveryNodes nodes, DocOrigin origin) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(origin.clusterAlias()) == false) {
            logger.debug("can't free fetch context [{}] of the remote cluster [{}]", origin.contextId(), origin.clusterAlias());
            return;
        }
        DiscoveryNode node = nodes.get(origin.nodeId());
        if (node == null) {
            // the node left the cluster, and took its contexts with it
            logger.debug("can't free fetch context [{}], node [{}] left the cluster", origin.contextId(), origin.nodeId());
            return;
        }
        try {
            searchTransportService.sendFreeContext(
                searchTransportService.transportService().getConnection(node),
                origin.contextId(),
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.debug(() -> Strings.format("failed to free fetch context [%s] on [%s]", origin.contextId(), node), e)
                )
            );
        } catch (Exception e) {
            logger.debug(() -> Strings.format("failed to free fetch context [%s] on [%s]", origin.contextId(), node), e);
        }
    }
}
