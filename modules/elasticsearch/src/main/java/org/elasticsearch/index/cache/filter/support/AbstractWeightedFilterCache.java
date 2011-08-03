/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.cache.filter.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.concurrentlinkedhashmap.EvictionListener;
import org.elasticsearch.common.concurrentlinkedhashmap.Weigher;
import org.elasticsearch.common.lab.LongsLAB;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractWeightedFilterCache extends AbstractIndexComponent implements FilterCache, IndexReader.ReaderFinishedListener, EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> {

    final ConcurrentMap<Object, Boolean> seenReaders = ConcurrentCollections.newConcurrentMap();
    final AtomicInteger seenReadersCount = new AtomicInteger();

    final boolean labEnabled;
    final ByteSizeValue labMaxAlloc;
    final ByteSizeValue labChunkSize;

    final int labMaxAllocBytes;
    final int labChunkSizeBytes;

    protected final AtomicLong evictions = new AtomicLong();

    final AtomicLong totalSizeInBytes = new AtomicLong();
    final AtomicInteger totalCount = new AtomicInteger();

    protected AbstractWeightedFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);

        // The LAB is stored per reader, so whole chunks will be cleared once reader is discarded.
        // This means that with filter entry specific based eviction, like access time
        // we might get into cases where the LAB is held by a puny filter and other filters have been released.
        // This usually will not be that bad, compared to the GC benefit of using a LAB, but, that is why
        // the soft filter cache is recommended.
        this.labEnabled = componentSettings.getAsBoolean("lab", false);
        // These values should not be too high, basically we want to cached the small readers and use the LAB for
        // them, 1M docs on OpenBitSet is around 110kb.
        this.labMaxAlloc = componentSettings.getAsBytesSize("lab.max_alloc", new ByteSizeValue(128, ByteSizeUnit.KB));
        this.labChunkSize = componentSettings.getAsBytesSize("lab.chunk_size", new ByteSizeValue(1, ByteSizeUnit.MB));

        this.labMaxAllocBytes = (int) (labMaxAlloc.bytes() / RamUsage.NUM_BYTES_LONG);
        this.labChunkSizeBytes = (int) (labChunkSize.bytes() / RamUsage.NUM_BYTES_LONG);
    }

    protected abstract ConcurrentMap<FilterCacheKey, FilterCacheValue<DocSet>> cache();

    @Override public void close() throws ElasticSearchException {
        clear();
    }

    @Override public void clear() {
        for (Object readerKey : seenReaders.keySet()) {
            Boolean removed = seenReaders.remove(readerKey);
            if (removed == null) {
                return;
            }
            seenReadersCount.decrementAndGet();
            ConcurrentMap<FilterCacheKey, FilterCacheValue<DocSet>> cache = cache();
            for (FilterCacheKey key : cache.keySet()) {
                if (key.readerKey() == readerKey) {
                    FilterCacheValue<DocSet> removed2 = cache.remove(key);
                    if (removed2 != null) {
                        totalCount.decrementAndGet();
                        totalSizeInBytes.addAndGet(-removed2.value().sizeInBytes());
                    }
                }
            }
        }
    }

    @Override public void finished(IndexReader reader) {
        clear(reader);
    }

    @Override public void clear(IndexReader reader) {
        // we add the seen reader before we add the first cache entry for this reader
        // so, if we don't see it here, its won't be in the cache
        Boolean removed = seenReaders.remove(reader.getCoreCacheKey());
        if (removed == null) {
            return;
        }
        seenReadersCount.decrementAndGet();
        ConcurrentMap<FilterCacheKey, FilterCacheValue<DocSet>> cache = cache();
        for (FilterCacheKey key : cache.keySet()) {
            if (key.readerKey() == reader.getCoreCacheKey()) {
                FilterCacheValue<DocSet> removed2 = cache.remove(key);
                if (removed2 != null) {
                    totalCount.decrementAndGet();
                    totalSizeInBytes.addAndGet(-removed2.value().sizeInBytes());
                }
            }
        }
    }

    @Override public EntriesStats entriesStats() {
        int seenReadersCount = this.seenReadersCount.get();
        return new EntriesStats(totalSizeInBytes.get(), seenReadersCount == 0 ? 0 : totalCount.get() / seenReadersCount);
    }

    @Override public long evictions() {
        return evictions.get();
    }

    @Override public Filter cache(Filter filterToCache) {
        if (filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    @Override public boolean isCached(Filter filter) {
        return filter instanceof FilterCacheFilterWrapper;
    }

    static class FilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private final AbstractWeightedFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, AbstractWeightedFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            Object filterKey = filter;
            if (filter instanceof CacheKeyFilter) {
                filterKey = ((CacheKeyFilter) filter).cacheKey();
            }
            FilterCacheKey cacheKey = new FilterCacheKey(reader.getCoreCacheKey(), filterKey);
            ConcurrentMap<FilterCacheKey, FilterCacheValue<DocSet>> innerCache = cache.cache();

            FilterCacheValue<DocSet> cacheValue = innerCache.get(cacheKey);
            if (cacheValue == null) {
                if (!cache.seenReaders.containsKey(reader.getCoreCacheKey())) {
                    Boolean previous = cache.seenReaders.putIfAbsent(reader.getCoreCacheKey(), Boolean.TRUE);
                    if (previous == null) {
                        reader.addReaderFinishedListener(cache);
                        cache.seenReadersCount.incrementAndGet();
                    }
                }

                LongsLAB longsLAB = null;
                if (cache.labEnabled) {
                    longsLAB = new LongsLAB(cache.labChunkSizeBytes, cache.labMaxAllocBytes);
                }
                DocIdSet docIdSet = filter.getDocIdSet(reader);
                DocSet docSet = FilterCacheValue.cacheable(reader, longsLAB, docIdSet);
                cacheValue = new FilterCacheValue<DocSet>(docSet, longsLAB);
                FilterCacheValue<DocSet> previous = innerCache.putIfAbsent(cacheKey, cacheValue);
                if (previous == null) {
                    cache.totalSizeInBytes.addAndGet(cacheValue.value().sizeInBytes());
                    cache.totalCount.incrementAndGet();
                }
            }

            return cacheValue.value() == DocSet.EMPTY_DOC_SET ? null : cacheValue.value();
        }

        public String toString() {
            return "FilterCacheFilterWrapper(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof FilterCacheFilterWrapper)) return false;
            return this.filter.equals(((FilterCacheFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF25;
        }
    }


    // factored by 10
    public static class FilterCacheValueWeigher implements Weigher<FilterCacheValue<DocSet>> {

        public static final long FACTOR = 10l;

        @Override public int weightOf(FilterCacheValue<DocSet> value) {
            int weight = (int) Math.min(value.value().sizeInBytes() / 10, Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    @Override public void onEviction(FilterCacheKey filterCacheKey, FilterCacheValue<DocSet> docSetFilterCacheValue) {
        if (filterCacheKey != null) {
            if (seenReaders.containsKey(filterCacheKey.readerKey())) {
                evictions.incrementAndGet();
                totalCount.decrementAndGet();
                if (docSetFilterCacheValue != null) {
                    totalSizeInBytes.addAndGet(-docSetFilterCacheValue.value().sizeInBytes());
                }
            }
        }
    }

    public static class FilterCacheKey {
        private final Object readerKey;
        private final Object filterKey;

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

        @Override public boolean equals(Object o) {
            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
            FilterCacheKey that = (FilterCacheKey) o;
            return (readerKey == that.readerKey && filterKey.equals(that.filterKey));
        }

        @Override public int hashCode() {
            return readerKey.hashCode() + 31 * filterKey.hashCode();
        }
    }
}