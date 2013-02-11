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
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class WeightedFilterCache extends AbstractIndexComponent implements FilterCache, SegmentReader.CoreClosedListener, RemovalListener<WeightedFilterCache.FilterCacheKey, DocSet> {

    final IndicesFilterCache indicesFilterCache;

    final ConcurrentMap<Object, Boolean> seenReaders = ConcurrentCollections.newConcurrentMap();
    final CounterMetric seenReadersCount = new CounterMetric();

    final CounterMetric evictionsMetric = new CounterMetric();
    final MeanMetric totalMetric = new MeanMetric();

    @Inject
    public WeightedFilterCache(Index index, @IndexSettings Settings indexSettings, IndicesFilterCache indicesFilterCache) {
        super(index, indexSettings);
        this.indicesFilterCache = indicesFilterCache;
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
            seenReadersCount.dec();
            indicesFilterCache.addReaderKeyToClean(readerKey);
        }
    }

    @Override
    public void onClose(SegmentReader owner) {
        clear(owner);
    }

    @Override
    public void clear(IndexReader reader) {
        // we add the seen reader before we add the first cache entry for this reader
        // so, if we don't see it here, its won't be in the cache
        Boolean removed = seenReaders.remove(reader.getCoreCacheKey());
        if (removed == null) {
            return;
        }
        seenReadersCount.dec();
        indicesFilterCache.addReaderKeyToClean(reader.getCoreCacheKey());
    }

    @Override
    public EntriesStats entriesStats() {
        long seenReadersCount = this.seenReadersCount.count();
        return new EntriesStats(totalMetric.sum(), seenReadersCount == 0 ? 0 : totalMetric.count() / seenReadersCount);
    }

    @Override
    public long evictions() {
        return evictionsMetric.count();
    }

    @Override
    public Filter cache(Filter filterToCache) {
        if (filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    @Override
    public boolean isCached(Filter filter) {
        return filter instanceof FilterCacheFilterWrapper;
    }

    static class FilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private final WeightedFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, WeightedFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override
        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            Object filterKey = filter;
            if (filter instanceof CacheKeyFilter) {
                filterKey = ((CacheKeyFilter) filter).cacheKey();
            }
            FilterCacheKey cacheKey = new FilterCacheKey(this.cache, reader.getCoreCacheKey(), filterKey);
            Cache<FilterCacheKey, DocSet> innerCache = cache.indicesFilterCache.cache();

            DocSet cacheValue = innerCache.getIfPresent(cacheKey);
            if (cacheValue == null) {
                if (!cache.seenReaders.containsKey(reader.getCoreCacheKey())) {
                    Boolean previous = cache.seenReaders.putIfAbsent(reader.getCoreCacheKey(), Boolean.TRUE);
                    if (previous == null && (reader instanceof SegmentReader)) {
                        ((SegmentReader) reader).addCoreClosedListener(cache);
                        cache.seenReadersCount.inc();
                    }
                }

                cacheValue = DocSets.cacheable(reader, filter.getDocIdSet(reader));
                // we might put the same one concurrently, that's fine, it will be replaced and the removal
                // will be called
                cache.totalMetric.inc(cacheValue.sizeInBytes());
                innerCache.put(cacheKey, cacheValue);
            }

            // return null if its EMPTY, this allows for further optimizations to ignore filters
            return cacheValue == DocSet.EMPTY_DOC_SET ? null : cacheValue;
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


    public static class FilterCacheValueWeigher implements Weigher<WeightedFilterCache.FilterCacheKey, DocSet> {

        @Override
        public int weigh(FilterCacheKey key, DocSet value) {
            int weight = (int) Math.min(value.sizeInBytes(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    // this will only be called for our index / data, IndicesFilterCache makes sure it works like this based on the
    // index we register the listener with
    @Override
    public void onRemoval(RemovalNotification<FilterCacheKey, DocSet> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictionsMetric.inc();
        }
        if (removalNotification.getValue() != null) {
            totalMetric.dec(removalNotification.getValue().sizeInBytes());
        }
    }

    public static class FilterCacheKey {
        private final RemovalListener<WeightedFilterCache.FilterCacheKey, DocSet> removalListener;
        private final Object readerKey;
        private final Object filterKey;

        public FilterCacheKey(RemovalListener<WeightedFilterCache.FilterCacheKey, DocSet> removalListener, Object readerKey, Object filterKey) {
            this.removalListener = removalListener;
            this.readerKey = readerKey;
            this.filterKey = filterKey;
        }

        public RemovalListener<WeightedFilterCache.FilterCacheKey, DocSet> removalListener() {
            return removalListener;
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
            return (readerKey.equals(that.readerKey) && filterKey.equals(that.filterKey));
        }

        @Override
        public int hashCode() {
            return readerKey.hashCode() + 31 * filterKey.hashCode();
        }
    }
}