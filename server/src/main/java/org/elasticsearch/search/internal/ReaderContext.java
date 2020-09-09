/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
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

/**
 * Holds a reference to a point in time {@link Engine.Searcher} that will be used to construct {@link SearchContext}.
 * This class also implements {@link org.elasticsearch.common.util.concurrent.RefCounted} since in some situations like
 * in {@link org.elasticsearch.search.SearchService} a SearchContext can be closed concurrently due to independent events
 * ie. when an index gets removed. To prevent accessing closed IndexReader / IndexSearcher instances the SearchContext
 * can be guarded by a reference count and fail if it's been closed by an external event.
 */
public class ReaderContext implements Releasable {
    private final ShardSearchContextId id;
    private final IndexService indexService;
    private final IndexShard indexShard;
    protected final Engine.SearcherSupplier searcherSupplier;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean singleSession;

    private final AtomicLong keepAlive;
    private final AtomicLong lastAccessTime;
    // For reference why we use RefCounted here see https://github.com/elastic/elasticsearch/pull/20095.
    private final AbstractRefCounted refCounted;

    private final List<Releasable> onCloses = new CopyOnWriteArrayList<>();

    private final long startTimeInNano = System.nanoTime();

    private Map<String, Object> context;

    public ReaderContext(long id,
                         IndexService indexService,
                         IndexShard indexShard,
                         Engine.SearcherSupplier searcherSupplier,
                         long keepAliveInMillis,
                         boolean singleSession) {
        this.id = new ShardSearchContextId(UUIDs.base64UUID(), id);
        this.indexService = indexService;
        this.indexShard = indexShard;
        this.searcherSupplier = searcherSupplier;
        this.singleSession = singleSession;
        this.keepAlive = new AtomicLong(keepAliveInMillis);
        this.lastAccessTime = new AtomicLong(nowInMillis());
        this.refCounted = new AbstractRefCounted("reader_context") {
            @Override
            protected void closeInternal() {
                doClose();
            }
        };
    }

    public void validate(TransportRequest request) {
        indexShard.getSearchOperationListener().validateReaderContext(this, request);
    }

    private long nowInMillis() {
        return indexShard.getThreadPool().relativeTimeInMillis();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            refCounted.decRef();
        }
    }

    void doClose() {
        Releasables.close(Releasables.wrap(onCloses), searcherSupplier);
    }

    public void addOnClose(Releasable releasable) {
        onCloses.add(releasable);
    }

    public ShardSearchContextId id() {
        return id;
    }

    public IndexService indexService() {
        return indexService;
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public Engine.Searcher acquireSearcher(String source) {
        return searcherSupplier.acquireSearcher(source);
    }

    private void tryUpdateKeepAlive(long keepAlive) {
        this.keepAlive.updateAndGet(curr -> Math.max(curr, keepAlive));
    }

    /**
     * Returns a releasable to indicate that the caller has stopped using this reader.
     * The time to live of the reader after usage can be extended using the provided
     * <code>keepAliveInMillis</code>.
     */
    public Releasable markAsUsed(long keepAliveInMillis) {
        refCounted.incRef();
        tryUpdateKeepAlive(keepAliveInMillis);
        return Releasables.releaseOnce(() -> {
            this.lastAccessTime.updateAndGet(curr -> Math.max(curr, nowInMillis()));
            refCounted.decRef();
        });
    }

    public boolean isExpired() {
        if (refCounted.refCount() > 1) {
            return false; // being used by markAsUsed
        }
        final long elapsed = nowInMillis() - lastAccessTime.get();
        return elapsed > keepAlive.get();
    }

    // BWC
    public ShardSearchRequest getShardSearchRequest(ShardSearchRequest other) {
        return Objects.requireNonNull(other);
    }

    public ScrollContext scrollContext() {
        return null;
    }

    public AggregatedDfs getAggregatedDfs(AggregatedDfs other) {
        return other;
    }

    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {

    }

    public RescoreDocIds getRescoreDocIds(RescoreDocIds other) {
        return Objects.requireNonNull(other);
    }

    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {

    }

    /**
     * Returns {@code true} for readers that are intended to use in a single query. For readers that are intended
     * to use in multiple queries (i.e., scroll or readers), we should not release them after the fetch phase
     * or the query phase with empty results.
     */
    public boolean singleSession() {
        return singleSession;
    }

    /**
     * Returns the object or <code>null</code> if the given key does not have a
     * value in the context
     */
    @SuppressWarnings("unchecked") // (T)object
    public <T> T getFromContext(String key) {
        return context != null ? (T) context.get(key) : null;
    }

    /**
     * Puts the object into the context
     */
    public void putInContext(String key, Object value) {
        if (context == null) {
            context = new HashMap<>();
        }
        context.put(key, value);
    }

    public long getStartTimeInNano() {
        return startTimeInNano;
    }
}
