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

package org.elasticsearch.indices.cache.request;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader version is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 */
public class IndicesRequestCache extends AbstractComponent implements RemovalListener<IndicesRequestCache.Key, IndicesRequestCache.Value> {

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetaData always.
     */
    public static final String INDEX_CACHE_REQUEST_ENABLED = "index.requests.cache.enable";
    public static final String INDICES_CACHE_REQUEST_CLEAN_INTERVAL = "indices.requests.cache.clean_interval";

    public static final String INDICES_CACHE_QUERY_SIZE = "indices.requests.cache.size";
    public static final String INDICES_CACHE_QUERY_EXPIRE = "indices.requests.cache.expire";

    private static final Set<SearchType> CACHEABLE_SEARCH_TYPES = EnumSet.of(SearchType.QUERY_THEN_FETCH, SearchType.QUERY_AND_FETCH);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    private final TimeValue cleanInterval;
    private final Reaper reaper;

    final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();


    //TODO make these changes configurable on the cluster level
    private final String size;
    private final TimeValue expire;

    private volatile Cache<Key, Value> cache;

    @Inject
    public IndicesRequestCache(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.cleanInterval = settings.getAsTime(INDICES_CACHE_REQUEST_CLEAN_INTERVAL, TimeValue.timeValueSeconds(60));

        this.size = settings.get(INDICES_CACHE_QUERY_SIZE, "1%");

        this.expire = settings.getAsTime(INDICES_CACHE_QUERY_EXPIRE, null);
        buildCache();

        this.reaper = new Reaper();
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, reaper);
    }

    private boolean isCacheEnabled(Settings settings, boolean defaultEnable) {
        return settings.getAsBoolean(INDEX_CACHE_REQUEST_ENABLED, defaultEnable);
    }

    private void buildCache() {
        long sizeInBytes = MemorySizeValue.parseBytesSizeValueOrHeapRatio(size, INDICES_CACHE_QUERY_SIZE).bytes();

        CacheBuilder<Key, Value> cacheBuilder = CacheBuilder.<Key, Value>builder()
                .setMaximumWeight(sizeInBytes).weigher((k, v) -> k.ramBytesUsed() + v.ramBytesUsed()).removalListener(this);
        // cacheBuilder.concurrencyLevel(concurrencyLevel);

        if (expire != null) {
            cacheBuilder.setExpireAfterAccess(TimeUnit.MILLISECONDS.toNanos(expire.millis()));
        }

        cache = cacheBuilder.build();
    }

    public void close() {
        reaper.close();
        cache.invalidateAll();
    }

    public void clear(IndexShard shard) {
        if (shard == null) {
            return;
        }
        keysToClean.add(new CleanupKey(shard, -1));
        logger.trace("{} explicit cache clear", shard.shardId());
        reaper.reap();
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Value> notification) {
        notification.getKey().shard.requestCache().onRemoval(notification);
    }

    /**
     * Can the shard request be cached at all?
     */
    public boolean canCache(ShardSearchRequest request, SearchContext context) {
        if (request.template() != null) {
            return false;
        }

        // for now, only enable it for requests with no hits
        if (context.size() != 0) {
            return false;
        }

        // We cannot cache with DFS because results depend not only on the content of the index but also
        // on the overridden statistics. So if you ran two queries on the same index with different stats
        // (because an other shard was updated) you would get wrong results because of the scores
        // (think about top_hits aggs or scripts using the score)
        if (!CACHEABLE_SEARCH_TYPES.contains(context.searchType())) {
            return false;
        }

        IndexMetaData index = clusterService.state().getMetaData().index(request.index());
        if (index == null) { // in case we didn't yet have the cluster state, or it just got deleted
            return false;
        }
        // if not explicitly set in the request, use the index setting, if not, use the request
        if (request.requestCache() == null) {
            if (!isCacheEnabled(index.getSettings(), Boolean.FALSE)) {
                return false;
            }
        } else if (!request.requestCache()) {
            return false;
        }
        // if the reader is not a directory reader, we can't get the version from it
        if (!(context.searcher().getIndexReader() instanceof DirectoryReader)) {
            return false;
        }
        // if now in millis is used (or in the future, a more generic "isDeterministic" flag
        // then we can't cache based on "now" key within the search request, as it is not deterministic
        if (context.nowInMillisUsed()) {
            return false;
        }
        return true;
    }

    /**
     * Loads the cache result, computing it if needed by executing the query phase and otherwise deserializing the cached
     * value into the {@link SearchContext#queryResult() context's query result}. The combination of load + compute allows
     * to have a single load operation that will cause other requests with the same key to wait till its loaded an reuse
     * the same cache.
     */
    public void loadIntoContext(final ShardSearchRequest request, final SearchContext context, final QueryPhase queryPhase) throws Exception {
        assert canCache(request, context);
        Key key = buildKey(request, context);
        Loader loader = new Loader(queryPhase, context);
        Value value = cache.computeIfAbsent(key, loader);
        if (loader.isLoaded()) {
            key.shard.requestCache().onMiss();
            // see if its the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(context.indexShard(), ((DirectoryReader) context.searcher().getIndexReader()).getVersion());
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    ElasticsearchDirectoryReader.addReaderCloseListener(context.searcher().getDirectoryReader(), cleanupKey);
                }
            }
        } else {
            key.shard.requestCache().onHit();
            // restore the cached query result into the context
            final QuerySearchResult result = context.queryResult();
            result.readFromWithId(context.id(), value.reference.streamInput());
            result.shardTarget(context.shardTarget());
        }
    }

    private static class Loader implements CacheLoader<Key, Value> {

        private final QueryPhase queryPhase;
        private final SearchContext context;
        private boolean loaded;

        Loader(QueryPhase queryPhase, SearchContext context) {
            this.queryPhase = queryPhase;
            this.context = context;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public Value load(Key key) throws Exception {
            queryPhase.execute(context);

            /* BytesStreamOutput allows to pass the expected size but by default uses
             * BigArrays.PAGE_SIZE_IN_BYTES which is 16k. A common cached result ie.
             * a date histogram with 3 buckets is ~100byte so 16k might be very wasteful
             * since we don't shrink to the actual size once we are done serializing.
             * By passing 512 as the expected size we will resize the byte array in the stream
             * slowly until we hit the page size and don't waste too much memory for small query
             * results.*/
            final int expectedSizeInBytes = 512;
            try (BytesStreamOutput out = new BytesStreamOutput(expectedSizeInBytes)) {
                context.queryResult().writeToNoId(out);
                // for now, keep the paged data structure, which might have unused bytes to fill a page, but better to keep
                // the memory properly paged instead of having varied sized bytes
                final BytesReference reference = out.bytes();
                loaded = true;
                Value value = new Value(reference, out.ramBytesUsed());
                key.shard.requestCache().onCached(key, value);
                return value;
            }
        }
    }

    public static class Value implements Accountable {
        final BytesReference reference;
        final long ramBytesUsed;

        public Value(BytesReference reference, long ramBytesUsed) {
            this.reference = reference;
            this.ramBytesUsed = ramBytesUsed;
        }

        @Override
        public long ramBytesUsed() {
            return ramBytesUsed;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    public static class Key implements Accountable {
        public final IndexShard shard; // use as identity equality
        public final long readerVersion; // use the reader version to now keep a reference to a "short" lived reader until its reaped
        public final BytesReference value;

        Key(IndexShard shard, long readerVersion, BytesReference value) {
            this.shard = shard;
            this.readerVersion = readerVersion;
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_LONG + value.length();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            // TODO: more detailed ram usage?
            return Collections.emptyList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            Key key = (Key) o;
            if (readerVersion != key.readerVersion) return false;
            if (!shard.equals(key.shard)) return false;
            if (!value.equals(key.value)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = shard.hashCode();
            result = 31 * result + Long.hashCode(readerVersion);
            result = 31 * result + value.hashCode();
            return result;
        }
    }

    private class CleanupKey implements IndexReader.ReaderClosedListener {
        IndexShard indexShard;
        long readerVersion; // use the reader version to now keep a reference to a "short" lived reader until its reaped

        private CleanupKey(IndexShard indexShard, long readerVersion) {
            this.indexShard = indexShard;
            this.readerVersion = readerVersion;
        }

        @Override
        public void onClose(IndexReader reader) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            CleanupKey that = (CleanupKey) o;
            if (readerVersion != that.readerVersion) return false;
            if (!indexShard.equals(that.indexShard)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexShard.hashCode();
            result = 31 * result + Long.hashCode(readerVersion);
            return result;
        }
    }

    private class Reaper implements Runnable {

        private final ObjectSet<CleanupKey> currentKeysToClean = new ObjectHashSet<>();
        private final ObjectSet<IndexShard> currentFullClean = new ObjectHashSet<>();

        private volatile boolean closed;

        void close() {
            closed = true;
        }

        @Override
        public void run() {
            if (closed) {
                return;
            }
            if (keysToClean.isEmpty()) {
                schedule();
                return;
            }
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        reap();
                        schedule();
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected", ex);
            }
        }

        private void schedule() {
            try {
                threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, this);
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not schedule ReaderCleaner - execution rejected", ex);
            }
        }

        synchronized void reap() {
            currentKeysToClean.clear();
            currentFullClean.clear();
            for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext(); ) {
                CleanupKey cleanupKey = iterator.next();
                iterator.remove();
                if (cleanupKey.readerVersion == -1 || cleanupKey.indexShard.state() == IndexShardState.CLOSED) {
                    // -1 indicates full cleanup, as does a closed shard
                    currentFullClean.add(cleanupKey.indexShard);
                } else {
                    currentKeysToClean.add(cleanupKey);
                }
            }

            if (!currentKeysToClean.isEmpty() || !currentFullClean.isEmpty()) {
                CleanupKey lookupKey = new CleanupKey(null, -1);
                for (Iterator<Key> iterator = cache.keys().iterator(); iterator.hasNext(); ) {
                    Key key = iterator.next();
                    if (currentFullClean.contains(key.shard)) {
                        iterator.remove();
                    } else {
                        lookupKey.indexShard = key.shard;
                        lookupKey.readerVersion = key.readerVersion;
                        if (currentKeysToClean.contains(lookupKey)) {
                            iterator.remove();
                        }
                    }
                }
            }

            cache.refresh();
            currentKeysToClean.clear();
            currentFullClean.clear();
        }
    }

    private static Key buildKey(ShardSearchRequest request, SearchContext context) throws Exception {
        // TODO: for now, this will create different keys for different JSON order
        // TODO: tricky to get around this, need to parse and order all, which can be expensive
        return new Key(context.indexShard(),
                ((DirectoryReader) context.searcher().getIndexReader()).getVersion(),
                request.cacheKey());
    }
}
