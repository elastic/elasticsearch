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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds a reference to a point in time {@link Engine.Searcher} that will be used to construct {@link SearchContext}.
 * This class also implements {@link org.elasticsearch.common.util.concurrent.RefCounted} since in some situations like
 * in {@link org.elasticsearch.search.SearchService} a SearchContext can be closed concurrently due to independent events
 * ie. when an index gets removed. To prevent accessing closed IndexReader / IndexSearcher instances the SearchContext
 * can be guarded by a reference count and fail if it's been closed by an external event.
 *
 * For reference why we use RefCounted here see https://github.com/elastic/elasticsearch/pull/20095.
 */
public class ReaderContext extends AbstractRefCounted implements Releasable {
    private final SearchContextId id;
    private final IndexShard indexShard;
    private final Engine.Reader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean singleSession;

    private final AtomicLong keepAlive;
    private final AtomicLong lastAccessTime;
    private final AtomicInteger accessors = new AtomicInteger();

    private final List<Releasable> onCloses = new CopyOnWriteArrayList<>();

    public ReaderContext(long id, IndexShard indexShard, Engine.Reader reader, long keepAliveInMillis, boolean singleSession) {
        super("reader_context");
        this.id = new SearchContextId(UUIDs.base64UUID(), id);
        this.indexShard = indexShard;
        this.reader = reader;
        this.singleSession = singleSession;
        this.keepAlive = new AtomicLong(keepAliveInMillis);
        this.lastAccessTime = new AtomicLong(nowInMillis());
    }

    private long nowInMillis() {
        return indexShard.getThreadPool().relativeTimeInMillis();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            decRef();
        }
    }

    @Override
    protected void closeInternal() {
        Releasables.close(Releasables.wrap(onCloses), reader);
    }

    public void addOnClose(Releasable releasable) {
        onCloses.add(releasable);
    }

    public SearchContextId id() {
        return id;
    }


    public IndexShard indexShard() {
        return indexShard;
    }

    public Engine.Searcher acquireSearcher(String source) {
        return reader.acquireSearcher(source);
    }

    public String source() {
        return "search";
    }

    public void keepAlive(long keepAlive) {
        this.keepAlive.updateAndGet(curr -> Math.max(curr, keepAlive));
    }

    /**
     * Marks this reader as being used so its time to live should not be expired.
     *
     * @return a releasable to indicate the caller has stopped using this reader
     */
    public Releasable markAsUsed() {
        accessors.incrementAndGet();
        return Releasables.releaseOnce(() -> {
            this.lastAccessTime.updateAndGet(curr -> Math.max(curr, nowInMillis()));
            accessors.decrementAndGet();
        });
    }

    public boolean isKeepAliveLapsed() {
        if (accessors.get() > 0) {
            return false; // being used
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
}
