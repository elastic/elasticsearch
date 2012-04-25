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
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.cache.filter.support.FilterCacheValue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class WeightedFilterCache extends AbstractIndexComponent implements FilterCache, SegmentReader.CoreClosedListener, RemovalListener<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> {

    final IndicesFilterCache indicesFilterCache;

    final ConcurrentMap<Object, Boolean> seenReaders = ConcurrentCollections.newConcurrentMap();
    final CounterMetric seenReadersCount = new CounterMetric();

    final CounterMetric evictionsMetric = new CounterMetric();
    final MeanMetric totalMetric = new MeanMetric();

    @Inject
    public WeightedFilterCache(Index index, @IndexSettings Settings indexSettings, IndicesFilterCache indicesFilterCache) {
        super(index, indexSettings);
        this.indicesFilterCache = indicesFilterCache;
        indicesFilterCache.addRemovalListener(index.name(), this);
    }

    @Override
    public String type() {
        return "weighted";
    }

    @Override
    public void close() throws ElasticSearchException {
        clear();
        indicesFilterCache.removeRemovalListener(index.name());
    }

    @Override
    public void clear() {
        for (Object readerKey : seenReaders.keySet()) {
            Boolean removed = seenReaders.remove(readerKey);
            if (removed == null) {
                return;
            }
            seenReadersCount.dec();
            for (FilterCacheKey key : indicesFilterCache.cache().asMap().keySet()) {
                if (key.readerKey() == readerKey) {
                    // invalidate will cause a removal and will be notified
                    indicesFilterCache.cache().invalidate(key);
                }
            }
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
        Cache<FilterCacheKey, FilterCacheValue<DocSet>> cache = indicesFilterCache.cache();
        for (FilterCacheKey key : cache.asMap().keySet()) {
            if (key.readerKey() == reader.getCoreCacheKey()) {
                // invalidate will cause a removal and will be notified
                cache.invalidate(key);
            }
        }
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
            FilterCacheKey cacheKey = new FilterCacheKey(cache.index().name(), reader.getCoreCacheKey(), filterKey);
            Cache<FilterCacheKey, FilterCacheValue<DocSet>> innerCache = cache.indicesFilterCache.cache();

            FilterCacheValue<DocSet> cacheValue = innerCache.getIfPresent(cacheKey);
            if (cacheValue == null) {
                if (!cache.seenReaders.containsKey(reader.getCoreCacheKey())) {
                    Boolean previous = cache.seenReaders.putIfAbsent(reader.getCoreCacheKey(), Boolean.TRUE);
                    if (previous == null) {
                        ((SegmentReader) reader).addCoreClosedListener(cache);
                        cache.seenReadersCount.inc();
                    }
                }

                DocIdSet docIdSet = filter.getDocIdSet(reader);
                DocSet docSet = FilterCacheValue.cacheable(reader, docIdSet);
                cacheValue = new FilterCacheValue<DocSet>(docSet);
                // we might put the same one concurrently, that's fine, it will be replaced and the removal
                // will be called
                cache.totalMetric.inc(cacheValue.value().sizeInBytes());
                innerCache.put(cacheKey, cacheValue);
            }

            return cacheValue.value() == DocSet.EMPTY_DOC_SET ? null : cacheValue.value();
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


    public static class FilterCacheValueWeigher implements Weigher<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> {

        @Override
        public int weigh(FilterCacheKey key, FilterCacheValue<DocSet> value) {
            int weight = (int) Math.min(value.value().sizeInBytes(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    // this will only be called for our index / data, IndicesFilterCache makes sure it works like this based on the
    // index we register the listener with
    @Override
    public void onRemoval(RemovalNotification<FilterCacheKey, FilterCacheValue<DocSet>> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictionsMetric.inc();
        }
        if (removalNotification.getValue() != null) {
            totalMetric.dec(removalNotification.getValue().value().sizeInBytes());
        }
    }

    public static class FilterCacheKey {
        private final String index;
        private final Object readerKey;
        private final Object filterKey;

        public FilterCacheKey(String index, Object readerKey, Object filterKey) {
            this.index = index;
            this.readerKey = readerKey;
            this.filterKey = filterKey;
        }

        public String index() {
            return index;
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
            return (readerKey == that.readerKey && filterKey.equals(that.filterKey));
        }

        @Override
        public int hashCode() {
            return readerKey.hashCode() + 31 * filterKey.hashCode();
        }
    }
}