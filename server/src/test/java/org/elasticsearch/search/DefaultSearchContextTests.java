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

package org.elasticsearch.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.test.ESTestCase;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSearchContextTests extends ESTestCase {

    public void testPreProcess() throws Exception {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);

        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        int maxSlicesPerScroll = randomIntBetween(50, 100);
        Settings settings = Settings.builder()
            .put("index.max_result_window", maxResultWindow)
            .put("index.max_slices_per_scroll", maxSlicesPerScroll)
            .put("index.max_rescore_window", maxRescoreWindow)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), anyObject(), anyObject(), anyString())).thenReturn(queryShardContext);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(randomBoolean());
        when(indexService.mapperService()).thenReturn(mapperService);

        IndexMetaData indexMetaData = IndexMetaData.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir);
             IndexReader reader = w.getReader();
             Engine.Searcher searcher = new Engine.Searcher("test", reader,
                 IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(),
                 IndexSearcher.getDefaultQueryCachingPolicy(), reader)) {

            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);

            DefaultSearchContext context1 = new DefaultSearchContext(1L, shardSearchRequest, target, searcher, null, indexService,
                indexShard, bigArrays, null, timeout, null, Version.CURRENT);
            context1.from(300);

            // resultWindow greater than maxResultWindow and scrollContext is null
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Result window is too large, from + size must be less than or equal to:"
                + " [" + maxResultWindow + "] but was [310]. See the scroll api for a more efficient way to request large data sets. "
                + "This limit can be set by changing the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                + "] index level setting."));

            // resultWindow greater than maxResultWindow and scrollContext isn't null
            context1.scrollContext(new ScrollContext());
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Batch size is too large, size must be less than or equal to: ["
                + maxResultWindow + "] but was [310]. Scroll batch sizes cost as much memory as result windows so they are "
                + "controlled by the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey() + "] index level setting."));

            // resultWindow not greater than maxResultWindow and both rescore and sort are not null
            context1.from(0);
            DocValueFormat docValueFormat = mock(DocValueFormat.class);
            SortAndFormats sortAndFormats = new SortAndFormats(new Sort(), new DocValueFormat[]{docValueFormat});
            context1.sort(sortAndFormats);

            RescoreContext rescoreContext = mock(RescoreContext.class);
            when(rescoreContext.getWindowSize()).thenReturn(500);
            context1.addRescore(rescoreContext);

            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

            // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
            context1.sort(null);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));

            assertThat(exception.getMessage(), equalTo("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
                + "It must be less than [" + maxRescoreWindow + "]. This prevents allocating massive heaps for storing the results "
                + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                + "] index level setting."));

            // rescore is null but sliceBuilder is not null
            DefaultSearchContext context2 = new DefaultSearchContext(2L, shardSearchRequest, target, searcher,
                null, indexService, indexShard, bigArrays, null, timeout, null, Version.CURRENT);

            SliceBuilder sliceBuilder = mock(SliceBuilder.class);
            int numSlices = maxSlicesPerScroll + randomIntBetween(1, 100);
            when(sliceBuilder.getMax()).thenReturn(numSlices);
            context2.sliceBuilder(sliceBuilder);

            exception = expectThrows(IllegalArgumentException.class, () -> context2.preProcess(false));
            assertThat(exception.getMessage(), equalTo("The number of slices [" + numSlices + "] is too large. It must "
                + "be less than [" + maxSlicesPerScroll + "]. This limit can be set by changing the [" +
                IndexSettings.MAX_SLICES_PER_SCROLL.getKey() + "] index level setting."));

            // No exceptions should be thrown
            when(shardSearchRequest.getAliasFilter()).thenReturn(AliasFilter.EMPTY);
            when(shardSearchRequest.indexBoost()).thenReturn(AbstractQueryBuilder.DEFAULT_BOOST);

            DefaultSearchContext context3 = new DefaultSearchContext(3L, shardSearchRequest, target, searcher, null,
                indexService, indexShard, bigArrays, null, timeout, null, Version.CURRENT);
            ParsedQuery parsedQuery = ParsedQuery.parsedMatchAllQuery();
            context3.sliceBuilder(null).parsedQuery(parsedQuery).preProcess(false);
            assertEquals(context3.query(), context3.buildFilteredQuery(parsedQuery.query()));
        }
    }
}
