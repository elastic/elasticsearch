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

package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.query.index.IndexQueryCache;

/**
 *
 */
public class QueryCacheModule extends AbstractModule {

    public static final class QueryCacheSettings {
        public static final String QUERY_CACHE_TYPE = "index.queries.cache.type";
        // for test purposes only
        public static final String QUERY_CACHE_EVERYTHING = "index.queries.cache.everything";
    }

    private final Settings settings;

    public QueryCacheModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(QueryCache.class)
                .to(settings.getAsClass(QueryCacheSettings.QUERY_CACHE_TYPE, IndexQueryCache.class, "org.elasticsearch.index.cache.query.", "QueryCache"))
                .in(Scopes.SINGLETON);
        // the query cache is a node-level thing, however we want the most popular queries
        // to be computed on a per-index basis, that is why we don't use the SINGLETON
        // scope below
        if (settings.getAsBoolean(QueryCacheSettings.QUERY_CACHE_EVERYTHING, false)) {
            bind(QueryCachingPolicy.class).toInstance(QueryCachingPolicy.ALWAYS_CACHE);
        } else {
            bind(QueryCachingPolicy.class).toInstance(new UsageTrackingQueryCachingPolicy());
        }
    }
}
