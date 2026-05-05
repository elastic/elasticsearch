/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.transport.TransportRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class SingleSessionReaderContext implements ReaderContext {
    private final ShardSearchContextId id;
    private final IndexService indexService;
    private final IndexShard indexShard;
    private final Engine.SearcherSupplier searcherSupplier;
    private final AtomicLong keepAlive;
    private final AtomicLong lastAccessTime;
    // For reference why we use RefCounted here see https://github.com/elastic/elasticsearch/pull/20095.
    private final AbstractRefCounted refCounted;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Releasable> onCloses = new CopyOnWriteArrayList<>();

    private final long startTimeInNano = System.nanoTime();

    private Map<String, Object> context;

    public SingleSessionReaderContext(
        ShardSearchContextId id,
        IndexService indexService,
        IndexShard indexShard,
        Engine.SearcherSupplier searcherSupplier,
        long keepAliveInMillis
    ) {
        this.id = id;
        this.indexService = indexService;
        this.indexShard = indexShard;
        this.searcherSupplier = searcherSupplier;
        this.keepAlive = new AtomicLong(keepAliveInMillis);
        this.lastAccessTime = new AtomicLong(nowInMillis());
        this.refCounted = AbstractRefCounted.of(this::doClose);
    }

    @Override
    public void validate(TransportRequest request) {
        indexShard.getSearchOperationListener().validateReaderContext(this, request);
    }

    @Override
    public void addOnClose(Releasable releasable) {
        onCloses.add(releasable);
    }

    @Override
    public ShardSearchContextId id() {
        return id;
    }

    @Override
    public IndexService indexService() {
        return indexService;
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    @Override
    public Engine.Searcher acquireSearcher(String source) {
        return searcherSupplier.acquireSearcher(source);
    }

    @Override
    public long keepAlive() {
        return keepAlive.longValue();
    }

    @Override
    public Releasable markAsUsed(long keepAliveInMillis) {
        refCounted.incRef();
        tryUpdateKeepAlive(keepAliveInMillis);
        return Releasables.releaseOnce(() -> {
            this.lastAccessTime.accumulateAndGet(nowInMillis(), Math::max);
            refCounted.decRef();
        });
    }

    @Override
    public boolean isExpired() {
        if (refCounted.refCount() > 1) {
            return false; // being used by markAsUsed
        }
        final long elapsed = nowInMillis() - lastAccessTime.get();
        return elapsed > keepAlive.get();
    }

    @Override
    public ShardSearchRequest getShardSearchRequest(ShardSearchRequest other) {
        return Objects.requireNonNull(other, "ShardSearchRequest must be sent back in a fetch request");
    }

    @Override
    public ScrollContext scrollContext() {
        return null;
    }

    @Override
    public AggregatedDfs getAggregatedDfs(AggregatedDfs other) {
        return other;
    }

    @Override
    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {

    }

    @Override
    public RescoreDocIds getRescoreDocIds(RescoreDocIds other) {
        return Objects.requireNonNull(other, "RescoreDocIds must be sent back in a fetch request");
    }

    @Override
    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {

    }

    @Override
    public boolean singleSession() {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked") // (T) object
    public <T> T getFromContext(String key) {
        return context != null ? (T) context.get(key) : null;
    }

    @Override
    public void putInContext(String key, Object value) {
        if (context == null) {
            context = new HashMap<>();
        }
        context.put(key, value);
    }

    @Override
    public long getStartTimeInNano() {
        return startTimeInNano;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            refCounted.decRef();
        }
    }

    private void doClose() {
        Releasables.close(Releasables.wrap(onCloses), searcherSupplier);
    }

    private long nowInMillis() {
        return indexShard.getThreadPool().relativeTimeInMillis();
    }

    private void tryUpdateKeepAlive(long keepAlive) {
        this.keepAlive.accumulateAndGet(keepAlive, Math::max);
    }
}
