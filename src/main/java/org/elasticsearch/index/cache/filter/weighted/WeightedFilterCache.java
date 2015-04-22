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

package org.elasticsearch.index.cache.filter.weighted;

import com.google.common.cache.Cache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.CachedFilter;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.ResolvableFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class WeightedFilterCache extends AbstractIndexComponent implements FilterCache, SegmentReader.CoreClosedListener, IndexReader.ReaderClosedListener {

    final IndicesFilterCache indicesFilterCache;
    IndexService indexService;

    final ConcurrentMap<Object, Boolean> seenReaders = ConcurrentCollections.newConcurrentMap();

    @Inject
    public WeightedFilterCache(Index index, @IndexSettings Settings indexSettings, IndicesFilterCache indicesFilterCache) {
        super(index, indexSettings);
        this.indicesFilterCache = indicesFilterCache;
    }

    @Override
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public String type() {
        return "weighted";
    }

    @Override
    public void close() throws ElasticsearchException {
        clear("close");
    }

    @Override
    public void onClose(IndexReader reader) {
        clear(reader.getCoreCacheKey());
    }


    @Override
    public void clear(String reason) {
        logger.debug("full cache clear, reason [{}]", reason);
        for (Object readerKey : seenReaders.keySet()) {
            Boolean removed = seenReaders.remove(readerKey);
            if (removed == null) {
                return;
            }
            indicesFilterCache.addReaderKeyToClean(readerKey);
        }
    }

    @Override
    public void clear(String reason, String[] keys) {
        logger.debug("clear keys [], reason [{}]", reason, keys);
        for (String key : keys) {
            final HashedBytesRef keyBytes = new HashedBytesRef(key);
            for (Object readerKey : seenReaders.keySet()) {
                indicesFilterCache.cache().invalidate(new FilterCacheKey(readerKey, keyBytes));
            }
        }
    }

    @Override
    public void onClose(Object coreKey) {
        clear(coreKey);
    }

    @Override
    public void clear(Object coreCacheKey) {
        // we add the seen reader before we add the first cache entry for this reader
        // so, if we don't see it here, its won't be in the cache
        Boolean removed = seenReaders.remove(coreCacheKey);
        if (removed == null) {
            return;
        }
        indicesFilterCache.addReaderKeyToClean(coreCacheKey);
    }

    @Override
    public Filter cache(Filter filterToCache, @Nullable HashedBytesRef cacheKey, QueryCachingPolicy cachePolicy) {
        if (filterToCache == null) {
            return null;
        }
        if (filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (CachedFilter.isCached(filterToCache)) {
            return filterToCache;
        }
        if (filterToCache instanceof ResolvableFilter) {
            throw new IllegalArgumentException("Cannot cache instances of ResolvableFilter: " + filterToCache);
        }
        return new FilterCacheFilterWrapper(filterToCache, cacheKey, cachePolicy, this);
    }

    static class FilterCacheFilterWrapper extends CachedFilter {

        private final Filter filter;
        private final Object filterCacheKey;
        private final QueryCachingPolicy cachePolicy;
        private final WeightedFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, Object cacheKey, QueryCachingPolicy cachePolicy, WeightedFilterCache cache) {
            this.filter = filter;
            this.filterCacheKey = cacheKey != null ? cacheKey : filter;
            this.cachePolicy = cachePolicy;
            this.cache = cache;
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            if (context.ord == 0) {
                cachePolicy.onUse(filter);
            }
            FilterCacheKey cacheKey = new FilterCacheKey(context.reader().getCoreCacheKey(), filterCacheKey);
            Cache<FilterCacheKey, DocIdSet> innerCache = cache.indicesFilterCache.cache();

            DocIdSet cacheValue = innerCache.getIfPresent(cacheKey);
            final DocIdSet ret;
            if (cacheValue != null) {
                ret = cacheValue;
            } else {
                final DocIdSet uncached = filter.getDocIdSet(context, null);
                if (cachePolicy.shouldCache(filter, context)) {
                    if (!cache.seenReaders.containsKey(context.reader().getCoreCacheKey())) {
                        Boolean previous = cache.seenReaders.putIfAbsent(context.reader().getCoreCacheKey(), Boolean.TRUE);
                        if (previous == null) {
                            // we add a core closed listener only, for non core IndexReaders we rely on clear being called (percolator for example)
                            context.reader().addCoreClosedListener(cache);
                        }
                    }
                    // we can't pass down acceptedDocs provided, because we are caching the result, and acceptedDocs
                    // might be specific to a query. We don't pass the live docs either because a cache built for a specific
                    // generation of a segment might be reused by an older generation which has fewer deleted documents
                    cacheValue = DocIdSets.toCacheable(context.reader(), uncached);
                    // we might put the same one concurrently, that's fine, it will be replaced and the removal
                    // will be called
                    ShardId shardId = ShardUtils.extractShardId(context.reader());
                    if (shardId != null) {
                        IndexShard shard = cache.indexService.shard(shardId.id());
                        if (shard != null) {
                            cacheKey.removalListener = shard.filterCache();
                            shard.filterCache().onCached(DocIdSets.sizeInBytes(cacheValue));
                        }
                    }
                    innerCache.put(cacheKey, cacheValue);
                    ret = cacheValue;
                } else {
                    // uncached
                    ret = uncached;
                }
            }

            return BitsFilteredDocIdSet.wrap(DocIdSets.isEmpty(ret) ? null : ret, acceptDocs);
        }

        @Override
        public String toString(String field) {
            return "cache(" + filter + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) return false;
            return this.filter.equals(((FilterCacheFilterWrapper) o).filter);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + filter.hashCode();
        }
    }


    /** A weigher for the Guava filter cache that uses a minimum entry size */
    public static class FilterCacheValueWeigher implements Weigher<WeightedFilterCache.FilterCacheKey, DocIdSet> {

        private final int minimumEntrySize;

        public FilterCacheValueWeigher(int minimumEntrySize) {
            this.minimumEntrySize = minimumEntrySize;
        }

        @Override
        public int weigh(FilterCacheKey key, DocIdSet value) {
            int weight = (int) Math.min(DocIdSets.sizeInBytes(value), Integer.MAX_VALUE);
            return Math.max(weight, this.minimumEntrySize);
        }
    }

    public static class FilterCacheKey {
        private final Object readerKey;
        private final Object filterKey;

        // if we know, we will try and set the removal listener (for statistics)
        // its ok that its not volatile because we make sure we only set it when the object is created before its shared between threads
        @Nullable
        public RemovalListener<WeightedFilterCache.FilterCacheKey, DocIdSet> removalListener;

        public FilterCacheKey(Object readerKey, Object filterKey) {
            this.readerKey = readerKey;
            this.filterKey = filterKey;
        }

        public Object readerKey() {
            return readerKey;
        }

        public Object filterKey() {
            return filterKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
            FilterCacheKey that = (FilterCacheKey) o;
            return (readerKey().equals(that.readerKey()) && filterKey.equals(that.filterKey));
        }

        @Override
        public int hashCode() {
            return readerKey().hashCode() + 31 * filterKey.hashCode();
        }
    }
}
