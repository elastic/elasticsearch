/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Follows the reader contexts that ES|QL opens for the fetch phase, on every index of a node. It binds each one to the
 * user who opened it before anyone can find it by id, sees every close whatever caused it, rejects other users and other
 * kinds of requests that look it up, and frees the contexts of a shard that closes on this node.
 * <p>
 * The listener runs for every reader context of the node, searches, scrolls and points in time included. It recognizes
 * the contexts of the fetch phase by the marker that {@link NodeFetchContexts} puts in the thread context around the
 * open, which runs the listener on the same thread.
 */
public final class FetchContextListener implements SearchOperationListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(FetchContextListener.class);

    /**
     * Thread context transient that marks the reader context being opened as a fetch context.
     */
    static final String OPEN_MARKER = "_esql_fetch_context_open";
    /**
     * Key of the marker in the context of a {@link ReaderContext}. Every fetch context has one.
     */
    static final String MARKER_KEY = "_esql_fetch_context";

    private static final long OWNER_MISMATCH_WARNING_INTERVAL_NANOS = TimeValue.timeValueMinutes(1).nanos();

    /**
     * Marks one open. The request that opens the context frees it, and hears when it closes.
     */
    static final class OpenMarker {
        private final NodeFetchContexts contexts;
        private volatile boolean bound;

        OpenMarker(NodeFetchContexts contexts) {
            this.contexts = contexts;
        }

        NodeFetchContexts contexts() {
            return contexts;
        }

        /**
         * Whether the listener bound a context to this marker. From then on the listener hears when the context closes,
         * also when the open fails after the binding and closes it.
         */
        boolean bound() {
            return bound;
        }
    }

    /**
     * Whether ES|QL opened {@code readerContext} for its fetch phase, rather than a search, a scroll or a point in time.
     */
    public static boolean isFetchContext(ReaderContext readerContext) {
        return readerContext.getFromContext(MARKER_KEY) != null;
    }

    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
    /**
     * The open fetch contexts of each shard. Each set changes only inside {@link Map#compute}, so an open never races
     * with the removal of an emptied set.
     */
    private final Map<ShardId, Set<ReaderContext>> openByShard = new ConcurrentHashMap<>();
    private final AtomicLong lastOwnerMismatchWarningNanos;

    public FetchContextListener(Settings settings, ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.securityContext = new SecurityContext(settings, threadContext);
        this.lastOwnerMismatchWarningNanos = new AtomicLong(System.nanoTime() - OWNER_MISMATCH_WARNING_INTERVAL_NANOS);
    }

    /**
     * Runs before the context is published, so a fetch context is never reachable by id without its owner. The owner
     * goes in before the marker: if reading the owner fails, the composite listener logs the failure and publishes the
     * context without a marker, which the open detects.
     */
    @Override
    public void onNewReaderContext(ReaderContext readerContext) {
        OpenMarker marker = threadContext.getTransient(OPEN_MARKER);
        if (marker == null) {
            return;
        }
        Authentication owner = securityContext.getAuthentication();
        if (owner != null) {
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, owner);
        }
        readerContext.putInContext(MARKER_KEY, marker);
        marker.bound = true;
        openByShard.compute(readerContext.indexShard().shardId(), (shardId, open) -> {
            Set<ReaderContext> contexts = open == null ? new HashSet<>() : open;
            contexts.add(readerContext);
            return contexts;
        });
    }

    /**
     * Runs once, when the last reference to the context is released, on whichever path freed it.
     */
    @Override
    public void onFreeReaderContext(ReaderContext readerContext) {
        OpenMarker marker = readerContext.getFromContext(MARKER_KEY);
        if (marker == null) {
            return;
        }
        assert Transports.assertNotTransportThread("closing a fetch context can do IO");
        openByShard.computeIfPresent(readerContext.indexShard().shardId(), (shardId, open) -> {
            open.remove(readerContext);
            return open.isEmpty() ? null : open;
        });
        marker.contexts().onReaderContextClosed(readerContext);
    }

    /**
     * Runs inside every lookup of a context by id. Both rejections report the context as missing, so a request can't
     * learn that it exists.
     */
    @Override
    public void validateReaderContext(ReaderContext readerContext, TransportRequest request) {
        if (readerContext.getFromContext(MARKER_KEY) == null) {
            return;
        }
        if (request instanceof FetchContextRequest == false) {
            throw new SearchContextMissingException(readerContext.id());
        }
        Authentication owner = readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY);
        if (securityContext.canIAccessResourcesCreatedBy(owner) == false) {
            warnOwnerMismatch(readerContext, request);
            throw new SearchContextMissingException(readerContext.id());
        }
    }

    /**
     * A fetch on a closed shard would build a search context over a closed index service, so the contexts of a shard
     * that closes on this node, for example because it relocated, go at once.
     */
    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        Set<ReaderContext> open = openByShard.remove(shardId);
        if (open == null) {
            return;
        }
        for (ReaderContext readerContext : open) {
            OpenMarker marker = readerContext.getFromContext(MARKER_KEY);
            marker.contexts().free(readerContext.id(), "its shard closed on this node");
        }
    }

    /**
     * Only a misbehaving or malicious caller presents another user's context, so the first one in a minute warns.
     */
    private void warnOwnerMismatch(ReaderContext readerContext, TransportRequest request) {
        User caller = securityContext.getUser();
        String principal = caller == null ? "unknown" : caller.principal();
        long now = System.nanoTime();
        long last = lastOwnerMismatchWarningNanos.get();
        if (now - last >= OWNER_MISMATCH_WARNING_INTERVAL_NANOS && lastOwnerMismatchWarningNanos.compareAndSet(last, now)) {
            logger.warn(
                "rejected [{}] for fetch context [{}] of shard [{}]: [{}] didn't open it",
                request.getClass().getSimpleName(),
                readerContext.id(),
                readerContext.indexShard().shardId(),
                principal
            );
        } else {
            logger.debug(
                "rejected [{}] for fetch context [{}] of shard [{}]: [{}] didn't open it",
                request.getClass().getSimpleName(),
                readerContext.id(),
                readerContext.indexShard().shardId(),
                principal
            );
        }
    }
}
