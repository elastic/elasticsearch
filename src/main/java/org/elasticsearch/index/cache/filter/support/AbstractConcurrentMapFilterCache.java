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
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.*;

/**
 * A base concurrent filter cache that accepts the actual cache to use.
 *
 * @author kimchy (shay.banon)
 */
public abstract class AbstractConcurrentMapFilterCache extends AbstractIndexComponent implements FilterCache, IndexReader.ReaderFinishedListener {

    final ConcurrentMap<Object, FilterCacheValue<ConcurrentMap<Object, DocSet>>> cache;

    protected AbstractConcurrentMapFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.cache = buildCache();
    }

    protected ConcurrentMap<Object, FilterCacheValue<ConcurrentMap<Object, DocSet>>> buildCache() {
        return new ConcurrentHashMap<Object, FilterCacheValue<ConcurrentMap<Object, DocSet>>>();
    }

    protected ConcurrentMap<Object, DocSet> buildFilterMap() {
        return newConcurrentMap();
    }

    @Override public void close() {
        cache.clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void finished(IndexReader reader) {
        FilterCacheValue<ConcurrentMap<Object, DocSet>> readerValue = cache.remove(reader.getCoreCacheKey());
        // help soft/weak handling GC
        if (readerValue != null) {
            readerValue.value().clear();
        }
    }

    @Override public void clear(IndexReader reader) {
        FilterCacheValue<ConcurrentMap<Object, DocSet>> readerValue = cache.remove(reader.getCoreCacheKey());
        // help soft/weak handling GC
        if (readerValue != null) {
            readerValue.value().clear();
        }
    }

    @Override public EntriesStats entriesStats() {
        long sizeInBytes = 0;
        long totalCount = 0;
        int segmentsCount = 0;
        for (FilterCacheValue<ConcurrentMap<Object, DocSet>> readerValue : cache.values()) {
            segmentsCount++;
            for (DocSet docSet : readerValue.value().values()) {
                sizeInBytes += docSet.sizeInBytes();
                totalCount++;
            }
        }
        return new EntriesStats(sizeInBytes, segmentsCount == 0 ? 0 : totalCount / segmentsCount);
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

    // LUCENE MONITOR: Check next version Lucene for CachingWrapperFilter, consider using that logic
    // and not use the DeletableConstantScoreQuery, instead pass the DeletesMode enum to the cache method
    // see: https://issues.apache.org/jira/browse/LUCENE-2468

    static class FilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private final AbstractConcurrentMapFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, AbstractConcurrentMapFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            FilterCacheValue<ConcurrentMap<Object, DocSet>> cacheValue = cache.cache.get(reader.getCoreCacheKey());
            if (cacheValue == null) {
                cacheValue = new FilterCacheValue<ConcurrentMap<Object, DocSet>>(cache.buildFilterMap());
                FilterCacheValue<ConcurrentMap<Object, DocSet>> prev = cache.cache.putIfAbsent(reader.getCoreCacheKey(), cacheValue);
                if (prev != null) {
                    cacheValue = prev;
                } else {
                    reader.addReaderFinishedListener(cache);
                }
            }
            Object key = filter;
            if (filter instanceof CacheKeyFilter) {
                key = ((CacheKeyFilter) filter).cacheKey();
            }

            DocSet docSet = cacheValue.value().get(key);
            if (docSet != null) {
                return docSet;
            }
            DocIdSet docIdSet = filter.getDocIdSet(reader);
            docSet = FilterCacheValue.cacheable(reader, docIdSet);
            DocSet prev = cacheValue.value().putIfAbsent(key, docSet);
            if (prev != null) {
                docSet = prev;
            }
            return docSet == DocSet.EMPTY_DOC_SET ? null : docSet;
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
}
