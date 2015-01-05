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

import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.ResolvableFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class WeightedFilterCache extends AbstractIndexComponent implements FilterCache {

    final IndicesFilterCache indicesFilterCache;
    IndexService indexService;

    final Set<Object> seenReaders = Collections.newSetFromMap(ConcurrentCollections.<Object, Boolean>newConcurrentMap());

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
    public void close() throws ElasticsearchException {
        clear("close");
    }

    @Override
    public void clear(String reason) {
        logger.debug("full cache clear, reason [{}]", reason);
        final List<Object> seenReaders = new ArrayList<>(this.seenReaders); // take a snapshot
        this.seenReaders.clear();
        indicesFilterCache.clearCoreCacheKeys(seenReaders);
    }

    @Override
    public Filter doCache(Filter filterToCache, FilterCachingPolicy cachePolicy) {
        while (filterToCache instanceof ReaderTrackingFilter) {
            filterToCache = ((ReaderTrackingFilter) filterToCache).uncached;
        }
        if (filterToCache == null || filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (filterToCache instanceof ResolvableFilter) {
            throw new IllegalArgumentException("Cannot cache instances of ResolvableFilter: " + filterToCache);
        }
        return new ReaderTrackingFilter(filterToCache, cachePolicy);
    }

    /**
     * We need this wrapper to track readers on which this cache has been used. This way,
     * we can clear only entries which are related to the current shard instead of all
     * filters which are cached on the node.
     */
    private class ReaderTrackingFilter extends Filter {

        final Filter uncached;
        final Filter cached;

        ReaderTrackingFilter(Filter uncached, FilterCachingPolicy cachePolicy) {
            this.uncached = uncached;
            this.cached = indicesFilterCache.doCache(uncached, cachePolicy);
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            if (ShardUtils.extractShardId(context.reader()) == null) {
                // If the reader does not expose a shard id, then we do not cache. The reason
                // is that the filter cache stats are computed by shard id and anything that
                // would not have a shard would take memory in the filter cache but would
                // be invisible to the stats API, so we rather not cache it...
                // For instance this happens when deleting by query.
                return uncached.getDocIdSet(context, acceptDocs);
            }
            if (seenReaders.add(context.reader().getCoreCacheKey())) {
                context.reader().addCoreClosedListener(new CoreClosedListener() {
                    @Override
                    public void onClose(Object ownerCoreCacheKey) throws IOException {
                        seenReaders.remove(ownerCoreCacheKey);
                    }
                });
            }
            return cached.getDocIdSet(context, acceptDocs);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ReaderTrackingFilter == false) {
                return false;
            }
            return uncached.equals(((ReaderTrackingFilter) obj).uncached);
        }

        @Override
        public int hashCode() {
            return uncached.hashCode() ^ getClass().hashCode();
        }

        @Override
        public String toString() {
            return cached.toString();
        }
    }
}
