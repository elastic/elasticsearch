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

package org.elasticsearch.index.cache.filter.none;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class NoneFilterCache extends AbstractIndexComponent implements FilterCache {

    @Inject
    public NoneFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        logger.debug("Using no filter cache");
    }

    @Override
    public void setIndexService(IndexService indexService) {
        // nothing to do here...
    }

    @Override
    public String type() {
        return "none";
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Filter cache(Filter filterToCache, @Nullable HashedBytesRef cacheKey, QueryCachingPolicy policy) {
        return filterToCache;
    }

    @Override
    public void clear(String reason) {
        // nothing to do here
    }

    @Override
    public void clear(String reason, String[] keys) {
        // nothing to do there
    }

    @Override
    public void clear(Object reader) {
        // nothing to do here
    }
}
