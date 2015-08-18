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
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.cache.query.index.IndexQueryCache;
import org.elasticsearch.index.cache.query.none.NoneQueryCache;

import java.io.IOException;

public class IndexCacheModuleTests extends ModuleTestCase {

    public void testCannotRegisterProvidedImplementations() {
        IndexCacheModule module = new IndexCacheModule(Settings.EMPTY);
        try {
            module.registerQueryCache("index", IndexQueryCache.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [index]");
        }

        try {
            module.registerQueryCache("none", NoneQueryCache.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [none]");
        }
    }

    public void testRegisterCustomQueryCache() {
        IndexCacheModule module = new IndexCacheModule(
                Settings.builder().put(IndexCacheModule.QUERY_CACHE_TYPE, "custom").build()
        );
        module.registerQueryCache("custom", CustomQueryCache.class);
        try {
            module.registerQueryCache("custom", CustomQueryCache.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [custom]");
        }
        assertBinding(module, QueryCache.class, CustomQueryCache.class);
    }

    public void testDefaultQueryCacheImplIsSelected() {
        IndexCacheModule module = new IndexCacheModule(Settings.EMPTY);
        assertBinding(module, QueryCache.class, IndexQueryCache.class);
    }

    class CustomQueryCache implements QueryCache {

        @Override
        public void clear(String reason) {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Index index() {
            return new Index("test");
        }

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            return weight;
        }
    }

}
