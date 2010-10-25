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
public abstract class AbstractDoubleConcurrentMapFilterCache extends AbstractIndexComponent implements FilterCache {

    final ConcurrentMap<Object, ConcurrentMap<Filter, DocSet>> cache;
    final ConcurrentMap<Object, ConcurrentMap<Filter, DocSet>> weakCache;

    protected AbstractDoubleConcurrentMapFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        // weak keys is fine, it will only be cleared once IndexReader references will be removed
        // (assuming clear(...) will not be called)
        this.cache = new MapMaker().weakKeys().makeMap();
        this.weakCache = new MapMaker().weakKeys().makeMap();
    }

    @Override public void close() {
        cache.clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void clear(IndexReader reader) {
        ConcurrentMap<Filter, DocSet> map = cache.remove(reader.getFieldCacheKey());
        // help soft/weak handling GC
        if (map != null) {
            map.clear();
        }
        map = weakCache.remove(reader.getFieldCacheKey());
        // help soft/weak handling GC
        if (map != null) {
            map.clear();
        }
    }

    @Override public void clearUnreferenced() {
    }

    @Override public Filter cache(Filter filterToCache) {
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    @Override public Filter weakCache(Filter filterToCache) {
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterWeakCacheFilterWrapper(filterToCache, this);
    }

    @Override public boolean isCached(Filter filter) {
        return filter instanceof CacheMarker;
    }

    protected ConcurrentMap<Filter, DocSet> buildCacheMap() {
        return newConcurrentMap();
    }

    protected ConcurrentMap<Filter, DocSet> buildWeakCacheMap() {
        return newConcurrentMap();
    }

    static abstract class CacheMarker extends Filter {

    }

    // LUCENE MONITOR: Check next version Lucene for CachingWrapperFilter, consider using that logic
    // and not use the DeletableConstantScoreQuery, instead pass the DeletesMode enum to the cache method
    // see: https://issues.apache.org/jira/browse/LUCENE-2468

    static class FilterCacheFilterWrapper extends CacheMarker {

        private final Filter filter;

        private final AbstractDoubleConcurrentMapFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, AbstractDoubleConcurrentMapFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            ConcurrentMap<Filter, DocSet> cachedFilters = cache.cache.get(reader.getFieldCacheKey());
            if (cachedFilters == null) {
                cachedFilters = cache.buildCacheMap();
                cache.cache.putIfAbsent(reader.getFieldCacheKey(), cachedFilters);
            }
            DocSet docSet = cachedFilters.get(filter);
            if (docSet != null) {
                return docSet;
            }

            // check if its in the weak cache, if so, move it from weak to soft
            ConcurrentMap<Filter, DocSet> weakCachedFilters = cache.weakCache.get(reader.getFieldCacheKey());
            if (weakCachedFilters != null) {
                docSet = weakCachedFilters.get(filter);
                if (docSet != null) {
                    cachedFilters.put(filter, docSet);
                    weakCachedFilters.remove(filter);
                    return docSet;
                }
            }

            DocIdSet docIdSet = filter.getDocIdSet(reader);
            docSet = cacheable(reader, docIdSet);
            cachedFilters.putIfAbsent(filter, docSet);
            return docIdSet;
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

    static class FilterWeakCacheFilterWrapper extends CacheMarker {

        private final Filter filter;

        private final AbstractDoubleConcurrentMapFilterCache cache;

        FilterWeakCacheFilterWrapper(Filter filter, AbstractDoubleConcurrentMapFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            DocSet docSet;
            // first check if its in the actual cache
            ConcurrentMap<Filter, DocSet> cachedFilters = cache.cache.get(reader.getFieldCacheKey());
            if (cachedFilters != null) {
                docSet = cachedFilters.get(filter);
                if (docSet != null) {
                    return docSet;
                }
            }

            // now, handle it in the weak cache
            ConcurrentMap<Filter, DocSet> weakCacheFilters = cache.weakCache.get(reader.getFieldCacheKey());
            if (weakCacheFilters == null) {
                weakCacheFilters = cache.buildWeakCacheMap();
                cache.weakCache.putIfAbsent(reader.getFieldCacheKey(), weakCacheFilters);
            }

            docSet = weakCacheFilters.get(filter);
            if (docSet != null) {
                return docSet;
            }

            DocIdSet docIdSet = filter.getDocIdSet(reader);
            docSet = cacheable(reader, docIdSet);
            weakCacheFilters.putIfAbsent(filter, docSet);
            return docIdSet;
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
