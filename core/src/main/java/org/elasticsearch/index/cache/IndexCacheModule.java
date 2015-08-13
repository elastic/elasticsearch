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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.cache.query.index.IndexQueryCache;
import org.elasticsearch.index.cache.query.none.NoneQueryCache;

public class IndexCacheModule extends AbstractModule {

    public static final String INDEX_QUERY_CACHE = "index";
    public static final String NONE_QUERY_CACHE = "none";
    public static final String QUERY_CACHE_TYPE = "index.queries.cache.type";
    // for test purposes only
    public static final String QUERY_CACHE_EVERYTHING = "index.queries.cache.everything";

    private final Settings settings;

    public IndexCacheModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        String queryCacheType = settings.get(QUERY_CACHE_TYPE, INDEX_QUERY_CACHE);
        Class<? extends QueryCache> queryCacheImpl;
        if (queryCacheType.equals(INDEX_QUERY_CACHE)) {
            queryCacheImpl = IndexQueryCache.class;
        } else if (queryCacheType.equals(NONE_QUERY_CACHE)) {
            queryCacheImpl = NoneQueryCache.class;
        } else {
            throw new IllegalArgumentException("Unknown QueryCache type [" + queryCacheType + "]");
        }
        bind(QueryCache.class).to(queryCacheImpl).in(Scopes.SINGLETON);
        bind(BitsetFilterCache.class).asEagerSingleton();
        bind(IndexCache.class).asEagerSingleton();
    }
}
