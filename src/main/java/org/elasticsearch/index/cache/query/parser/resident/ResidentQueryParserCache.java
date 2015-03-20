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

package org.elasticsearch.index.cache.query.parser.resident;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.parser.QueryParserCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.TimeUnit;

/**
 * A small (by default) query parser cache mainly to not parse the same query string several times
 * if several shards exists on the same node.
 */
public class ResidentQueryParserCache extends AbstractIndexComponent implements QueryParserCache {

    private final Cache<QueryParserSettings, Query> cache;

    private volatile int maxSize;
    private volatile TimeValue expire;

    @Inject
    public ResidentQueryParserCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);

        this.maxSize = indexSettings.getAsInt("index.cache.query.parser.resident.max_size", 100);
        this.expire = indexSettings.getAsTime("index.cache.query.parser.resident.expire", null);
        logger.debug("using [resident] query cache with max_size [{}], expire [{}]", maxSize, expire);

        CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(maxSize);
        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.nanos(), TimeUnit.NANOSECONDS);
        }

        this.cache = cacheBuilder.build();
    }

    @Override
    public Query get(QueryParserSettings queryString) {
        Query value =  cache.getIfPresent(queryString);
        if (value != null) {
            return value.clone();
        } else {
            return null;
        }
    }

    @Override
    public void put(QueryParserSettings queryString, Query query) {
        if (queryString.isCacheable()) {
            cache.put(queryString, query);
        }
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public void close() throws ElasticsearchException {
        cache.invalidateAll();
    }
}
