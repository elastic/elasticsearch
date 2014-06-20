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
package org.elasticsearch.indices.fielddata.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.BaseBogusReadersCacheTestCase;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.RamUsage;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.index.service.StubIndexService;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class FieldDataBaseBogusReadersCacheTest extends BaseBogusReadersCacheTestCase {

    @Test
    public void testFieldDataCache() throws Exception {
        Index index = new Index("test");
        StubIndexService indexService = new StubIndexService(null);
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        IndicesFieldDataCacheListener listener = new IndicesFieldDataCacheListener(breakerService);
        FieldDataType type = new FieldDataType("type");
        FieldMapper.Names names = new FieldMapper.Names("a");
        Cache<IndicesFieldDataCache.Key, RamUsage> cache = CacheBuilder.newBuilder().build();
        IndicesFieldDataCache.IndexFieldCache indexFieldCache = new IndicesFieldDataCache.IndexFieldCache(logger, cache, listener, indexService, index, names, type);
        PagedBytesIndexFieldData fd = new PagedBytesIndexFieldData(index, ImmutableSettings.EMPTY, names, type, indexFieldCache, breakerService, null);

        try {
            indexFieldCache.load(bogusContext, fd);
            fail();
        } catch (Exception e) {
            ElasticsearchIllegalStateException cause = (ElasticsearchIllegalStateException) e.getCause();
            assertThat(cause.getMessage(), equalTo("Can not extract segment reader from given index reader [SlowCompositeReaderWrapper(MultiReader())]"));
        }

        indexFieldCache.load(validContext, fd);
    }

}
