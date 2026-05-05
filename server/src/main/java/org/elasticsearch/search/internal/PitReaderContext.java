/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.transport.TransportRequest;

public final class PitReaderContext implements ReaderContext {
    private static final long CONTEXT_RELOCATION_GRACE_TIME_MS = 1000;

    private final ReaderContext delegate;

    // Volatile since it is written by generic thread pool thread (in `afterIndexShardClosed`)
    // and read from search threads.
    private volatile long relocatedTimestampMs = -1;

    public PitReaderContext(
        ShardSearchContextId id,
        IndexService indexService,
        IndexShard indexShard,
        Engine.SearcherSupplier searcherSupplier,
        long keepAliveInMillis
    ) {
        this.delegate = new SingleSessionReaderContext(id, indexService, indexShard, searcherSupplier, keepAliveInMillis);
    }

    @Override
    public void validate(TransportRequest request) {
        delegate.validate(request);
    }

    @Override
    public void addOnClose(Releasable releasable) {
        delegate.addOnClose(releasable);
    }

    @Override
    public ShardSearchContextId id() {
        return delegate.id();
    }

    @Override
    public IndexService indexService() {
        return delegate.indexService();
    }

    @Override
    public IndexShard indexShard() {
        return delegate.indexShard();
    }

    public long keepAlive() {
        return delegate.keepAlive();
    }

    @Override
    public Engine.Searcher acquireSearcher(String source) {
        return delegate.acquireSearcher(source);
    }

    @Override
    public Releasable markAsUsed(long keepAliveInMillis) {
        return delegate.markAsUsed(keepAliveInMillis);
    }

    @Override
    public boolean isExpired() {
        if (isRelocating()) {
            // Only for PIT contexts that are relocating away. We don't want to close immediately to
            // prevent running searches from failing. Refcounting via #markAsUsed protects against this
            // while search phases are running, but we also need to protect against closing too soon during
            // phase transitions. The grace period is long enough to allow for search phase transitions
            // of running searches before they complete.
            return nowInMillis() - relocatedTimestampMs > CONTEXT_RELOCATION_GRACE_TIME_MS;
        }

        return delegate.isExpired();
    }

    @Override
    public boolean isRelocating() {
        return relocatedTimestampMs > 0;
    }

    @Override
    public ShardSearchRequest getShardSearchRequest(ShardSearchRequest other) {
        return delegate.getShardSearchRequest(other);
    }

    @Override
    public ScrollContext scrollContext() {
        return delegate.scrollContext();
    }

    @Override
    public AggregatedDfs getAggregatedDfs(AggregatedDfs other) {
        return delegate.getAggregatedDfs(other);
    }

    @Override
    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {
        delegate.setAggregatedDfs(aggregatedDfs);
    }

    @Override
    public RescoreDocIds getRescoreDocIds(RescoreDocIds other) {
        return delegate.getRescoreDocIds(other);
    }

    @Override
    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        delegate.setRescoreDocIds(rescoreDocIds);
    }

    @Override
    public boolean singleSession() {
        return false;
    }

    @Override
    public <T> T getFromContext(String key) {
        return delegate.getFromContext(key);
    }

    @Override
    public void putInContext(String key, Object value) {
        delegate.putInContext(key, value);
    }

    @Override
    public long getStartTimeInNano() {
        return delegate.getStartTimeInNano();
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Indicate that this context is in the process of relocating.
     * We check this to prevent new search requests from using this context,
     * while running searches can still use it. Also this marks the context for cleanup
     * in one of the next {@link  SearchService} Reaper runs.
     */
    public void relocate() {
        relocatedTimestampMs = nowInMillis();
    }

    private long nowInMillis() {
        return indexShard().getThreadPool().relativeTimeInMillis();
    }
}
