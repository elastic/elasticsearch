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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.lucene.docset.DocSets.*;
import static org.elasticsearch.util.concurrent.ConcurrentCollections.*;

/**
 * A base concurrent filter cache that accepts the actual cache to use.
 *
 * @author kimchy (shay.banon)
 */
public abstract class AbstractConcurrentMapFilterCache extends AbstractIndexComponent implements FilterCache {

    private final ConcurrentMap<Object, ConcurrentMap<Filter, DocSet>> cache;

    protected AbstractConcurrentMapFilterCache(Index index, @IndexSettings Settings indexSettings,
                                               ConcurrentMap<Object, ConcurrentMap<Filter, DocSet>> cache) {
        super(index, indexSettings);
        this.cache = cache;
    }

    @Override public void close() {
        cache.clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void clear(IndexReader reader) {
        cache.remove(reader.getFieldCacheKey());
    }

    @Override public void clearUnreferenced() {
        // can't do this, since we cache on cacheKey...
//        int totalCount = cache.size();
//        int cleaned = 0;
//        for (Iterator<IndexReader> readerIt = cache.keySet().iterator(); readerIt.hasNext();) {
//            IndexReader reader = readerIt.next();
//            if (reader.getRefCount() <= 0) {
//                readerIt.remove();
//                cleaned++;
//            }
//        }
//        if (logger.isDebugEnabled()) {
//            if (cleaned > 0) {
//                logger.debug("Cleaned [{}] out of estimated total [{}]", cleaned, totalCount);
//            }
//        } else if (logger.isTraceEnabled()) {
//            logger.trace("Cleaned [{}] out of estimated total [{}]", cleaned, totalCount);
//        }
    }

    @Override public Filter cache(Filter filterToCache) {
        return new FilterCacheFilterWrapper(filterToCache);
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

    private class FilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private FilterCacheFilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            ConcurrentMap<Filter, DocSet> cachedFilters = cache.get(reader.getFieldCacheKey());
            if (cachedFilters == null) {
                cachedFilters = buildFilterMap();
                cache.putIfAbsent(reader.getFieldCacheKey(), cachedFilters);
            }
            DocSet docSet = cachedFilters.get(filter);
            if (docSet != null) {
                return docSet;
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
}
