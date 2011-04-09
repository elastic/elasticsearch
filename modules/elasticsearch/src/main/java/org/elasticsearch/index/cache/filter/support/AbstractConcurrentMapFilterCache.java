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
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.lucene.docset.DocSets.*;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.*;

/**
 * A base concurrent filter cache that accepts the actual cache to use.
 *
 * @author kimchy (shay.banon)
 */
public abstract class AbstractConcurrentMapFilterCache extends AbstractIndexComponent implements FilterCache {

    final ConcurrentMap<Object, ConcurrentMap<Filter, DocSet>> cache;

    protected AbstractConcurrentMapFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        // weak keys is fine, it will only be cleared once IndexReader references will be removed
        // (assuming clear(...) will not be called)
        this.cache = new MapMaker().weakKeys().makeMap();
    }

    @Override public void close() {
        cache.clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void clear(IndexReader reader) {
        ConcurrentMap<Filter, DocSet> map = cache.remove(reader.getCoreCacheKey());
        // help soft/weak handling GC
        if (map != null) {
            map.clear();
        }
    }

    @Override public long sizeInBytes() {
        long sizeInBytes = 0;
        for (ConcurrentMap<Filter, DocSet> map : cache.values()) {
            for (DocSet docSet : map.values()) {
                sizeInBytes += docSet.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override public long count() {
        long entries = 0;
        for (ConcurrentMap<Filter, DocSet> map : cache.values()) {
            entries += map.size();
        }
        return entries;
    }

    @Override public Filter cache(Filter filterToCache) {
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    @Override public boolean isCached(Filter filter) {
        return filter instanceof FilterCacheFilterWrapper;
    }

    protected ConcurrentMap<Filter, DocSet> buildFilterMap() {
        return newConcurrentMap();
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
            ConcurrentMap<Filter, DocSet> cachedFilters = cache.cache.get(reader.getCoreCacheKey());
            if (cachedFilters == null) {
                cachedFilters = cache.buildFilterMap();
                ConcurrentMap<Filter, DocSet> prev = cache.cache.putIfAbsent(reader.getCoreCacheKey(), cachedFilters);
                if (prev != null) {
                    cachedFilters = prev;
                }
            }
            DocSet docSet = cachedFilters.get(filter);
            if (docSet != null) {
                return docSet;
            }
            DocIdSet docIdSet = filter.getDocIdSet(reader);
            docSet = cacheable(reader, docIdSet);
            DocSet prev = cachedFilters.putIfAbsent(filter, docSet);
            if (prev != null) {
                docSet = prev;
            }
            return docSet;
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
