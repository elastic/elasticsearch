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

package org.elasticsearch.index.cache.filter;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.elasticsearch.index.IndexLifecycle;
import org.elasticsearch.index.cache.filter.soft.SoftFilterCache;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
@IndexLifecycle
public class FilterCacheModule extends AbstractModule {

    public static final class FilterCacheSettings {
        public static final String FILTER_CACHE_TYPE = "index.cache.filter.type";
    }

    private final Settings settings;

    public FilterCacheModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(FilterCache.class)
                .to(settings.getAsClass(FilterCacheSettings.FILTER_CACHE_TYPE, SoftFilterCache.class, "org.elasticsearch.index.cache.filter.", "FilterCache"))
                .in(Scopes.SINGLETON);
    }
}
