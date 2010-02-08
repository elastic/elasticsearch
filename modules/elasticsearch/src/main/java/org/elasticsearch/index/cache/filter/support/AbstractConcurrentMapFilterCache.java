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
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;
import static org.elasticsearch.util.lucene.docidset.DocIdSets.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractConcurrentMapFilterCache extends AbstractIndexComponent implements FilterCache {

    private final ConcurrentMap<IndexReader, ConcurrentMap<Filter, DocIdSet>> cache;

    private final TimeValue readerCleanerSchedule;

    private final Future scheduleFuture;

    protected AbstractConcurrentMapFilterCache(Index index, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(index, indexSettings);

        this.readerCleanerSchedule = componentSettings.getAsTime("readerCleanerSchedule", TimeValue.timeValueMinutes(1));

        logger.debug("Using weak filter cache with readerCleanerSchedule [{}]", readerCleanerSchedule);

        this.cache = newConcurrentMap();
        this.scheduleFuture = threadPool.scheduleWithFixedDelay(new IndexReaderCleaner(), readerCleanerSchedule);
    }

    @Override public void close() {
        scheduleFuture.cancel(false);
        cache.clear();
    }

    @Override public Filter cache(Filter filterToCache) {
        return new SoftFilterCacheFilterWrapper(filterToCache);
    }

    private class IndexReaderCleaner implements Runnable {
        @Override public void run() {
            for (Iterator<IndexReader> readerIt = cache.keySet().iterator(); readerIt.hasNext();) {
                IndexReader reader = readerIt.next();
                if (reader.getRefCount() <= 0) {
                    readerIt.remove();
                }
            }
        }
    }

    protected abstract ConcurrentMap buildMap();

    private class SoftFilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private SoftFilterCacheFilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            ConcurrentMap<Filter, DocIdSet> cachedFilters = cache.get(reader);
            if (cachedFilters == null) {
                cachedFilters = buildMap();
                cache.putIfAbsent(reader, cachedFilters);
            }
            DocIdSet docIdSet = cachedFilters.get(filter);
            if (docIdSet != null) {
                return docIdSet;
            }
            docIdSet = filter.getDocIdSet(reader);
            docIdSet = cacheable(reader, docIdSet);
            cachedFilters.putIfAbsent(filter, docIdSet);
            return docIdSet;
        }

        public String toString() {
            return "FilterCacheFilterWrapper(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof SoftFilterCacheFilterWrapper)) return false;
            return this.filter.equals(((SoftFilterCacheFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF25;
        }
    }
}
