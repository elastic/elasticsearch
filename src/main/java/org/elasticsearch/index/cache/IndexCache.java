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

package org.elasticsearch.index.cache;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public class IndexCache extends AbstractIndexComponent implements Closeable {

    private final FilterCache filterCache;
    private final QueryCachingPolicy filterCachingPolicy;
    private final BitsetFilterCache bitsetFilterCache;

    @Inject
    public IndexCache(Index index, @IndexSettings Settings indexSettings, FilterCache filterCache, QueryCachingPolicy filterCachingPolicy, BitsetFilterCache bitsetFilterCache) {
        super(index, indexSettings);
        this.filterCache = filterCache;
        this.filterCachingPolicy = filterCachingPolicy;
        this.bitsetFilterCache = bitsetFilterCache;
    }

    public FilterCache filter() {
        return filterCache;
    }

    public QueryCachingPolicy filterPolicy() {
        return filterCachingPolicy;
    }

    /**
     * Return the {@link BitsetFilterCache} for this index.
     */
    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(filterCache, bitsetFilterCache);
    }

    public void clear(String reason) {
        filterCache.clear(reason);
        bitsetFilterCache.clear(reason);
    }

}
