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

package org.elasticsearch.index.cache.filter;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;

/**
 *
 */
public class FilterCacheModule extends AbstractModule {

    public static final class FilterCacheSettings {
        public static final String FILTER_CACHE_TYPE = "index.cache.filter.type";
        // for test purposes only
        public static final String FILTER_CACHE_EVERYTHING = "index.cache.filter.everything";
    }

    private final Settings settings;

    public FilterCacheModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(FilterCache.class)
                .to(settings.getAsClass(FilterCacheSettings.FILTER_CACHE_TYPE, WeightedFilterCache.class, "org.elasticsearch.index.cache.filter.", "FilterCache"))
                .in(Scopes.SINGLETON);
        // the filter cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-index basis, that is why we don't use the SINGLETON
        // scope below
        if (settings.getAsBoolean(FilterCacheSettings.FILTER_CACHE_EVERYTHING, false)) {
            bind(QueryCachingPolicy.class).toInstance(QueryCachingPolicy.ALWAYS_CACHE);
        } else {
            bind(QueryCachingPolicy.class).toInstance(new UsageTrackingQueryCachingPolicy());
        }
    }
}
