/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.CachedFilter;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class WeightedFilterCache extends AbstractIndexComponent implements FilterCache, SegmentReader.CoreClosedListener {

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
    public void close() throws ElasticSearchException {
        clear("close");
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
        final BytesRef spare = new BytesRef();
        for (String key : keys) {
            final byte[] keyBytes = Strings.toUTF8Bytes(key, spare);
            for (Object readerKey : seenReaders.keySet()) {
                indicesFilterCache.cache().invalidate(new FilterCacheKey(readerKey, new CacheKeyFilter.Key(keyBytes)));
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
    public Filter cache(Filter filterToCache) {
        if (filterToCache == null) {
            return null;
        }
        if (filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (CachedFilter.isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    static class FilterCacheFilterWrapper extends CachedFilter {

        private final Filter filter;

        private final WeightedFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, WeightedFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }


        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            Object filterKey = filter;
            if (filter instanceof CacheKeyFilter) {
                filterKey = ((CacheKeyFilter) filter).cacheKey();
            }
            FilterCacheKey cacheKey = new FilterCacheKey(context.reader().getCoreCacheKey(), filterKey);
            Cache<FilterCacheKey, DocIdSet> innerCache = cache.indicesFilterCache.cache();

            DocIdSet cacheValue = innerCache.getIfPresent(cacheKey);
            if (cacheValue == null) {
                if (!cache.seenReaders.containsKey(context.reader().getCoreCacheKey())) {
                    Boolean previous = cache.seenReaders.putIfAbsent(context.reader().getCoreCacheKey(), Boolean.TRUE);
                    if (previous == null) {
                        // we add a core closed listener only, for non core IndexReaders we rely on clear being called (percolator for example)
                        if (context.reader() instanceof SegmentReader) {
                            ((SegmentReader) context.reader()).addCoreClosedListener(cache);
                        }
                    }
                }
                // we can't pass down acceptedDocs provided, because we are caching the result, and acceptedDocs
                // might be specific to a query. We don't pass the live docs either because a cache built for a specific
                // generation of a segment might be reused by an older generation which has fewer deleted documents
                cacheValue = DocIdSets.toCacheable(context.reader(), filter.getDocIdSet(context, null));
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
            }

            // note, we don't wrap the return value with a BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs) because
            // we rely on our custom XFilteredQuery to do the wrapping if needed, so we don't have the wrap each
            // filter on its own
            return DocIdSets.isEmpty(cacheValue) ? null : cacheValue;
        }

        public String toString() {
            return "cache(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof FilterCacheFilterWrapper)) return false;
            return this.filter.equals(((FilterCacheFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF25;
        }
    }


    public static class FilterCacheValueWeigher implements Weigher<WeightedFilterCache.FilterCacheKey, DocIdSet> {

        @Override
        public int weigh(FilterCacheKey key, DocIdSet value) {
            int weight = (int) Math.min(DocIdSets.sizeInBytes(value), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
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