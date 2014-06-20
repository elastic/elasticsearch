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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.BaseBogusReadersCacheTestCase;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class WeigthedBaseBogusReadersCacheTest extends BaseBogusReadersCacheTestCase {

    @Test
    public void testWeightedFilterCache() throws Exception {
        Index index = new Index("test");
        IndicesFilterCache indicesFilterCache = new IndicesFilterCache(
                EMPTY_SETTINGS, new ThreadPool(), new CacheRecycler(EMPTY_SETTINGS), new NodeSettingsService(EMPTY_SETTINGS)
        );
        WeightedFilterCache cache = new WeightedFilterCache(index, EMPTY_SETTINGS, indicesFilterCache);

        Filter filter = new TermFilter(new Term("a", "b"));
        filter = cache.cache(filter);

        try {
            filter.getDocIdSet(bogusContext, null);
            fail();
        } catch (ElasticsearchIllegalStateException e) {
            assertThat(e.getMessage(), equalTo("Can not extract segment reader from given index reader [SlowCompositeReaderWrapper(MultiReader())]"));
        }

        DocIdSet result = filter.getDocIdSet(validContext, null);
    }

}
