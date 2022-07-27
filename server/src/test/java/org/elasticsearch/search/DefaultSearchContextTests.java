/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSearchContextTests extends ESTestCase {

    public void testPreProcess() throws Exception {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);
        when(shardSearchRequest.shardRequestIndex()).thenReturn(shardId.id());
        when(shardSearchRequest.numberOfShards()).thenReturn(2);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        int maxSlicesPerScroll = randomIntBetween(50, 100);
        Settings settings = Settings.builder()
            .put("index.max_result_window", maxResultWindow)
            .put("index.max_slices_per_scroll", maxSlicesPerScroll)
            .put("index.max_rescore_window", maxRescoreWindow)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(indexService.newSearchExecutionContext(eq(shardId.id()), eq(shardId.id()), any(), any(), nullable(String.class), any()))
            .thenReturn(searchExecutionContext);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(randomBoolean());
        when(indexService.mapperService()).thenReturn(mapperService);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            final Supplier<Engine.SearcherSupplier> searcherSupplier = () -> new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {}

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher(
                            "test",
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(),
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            reader
                        );
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };

            SearchShardTarget target = new SearchShardTarget("node", shardId, null);

            ReaderContext readerWithoutScroll = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            DefaultSearchContext contextWithoutScroll = new DefaultSearchContext(
                readerWithoutScroll,
                shardSearchRequest,
                target,
                null,
                timeout,
                null,
                false
            );
            contextWithoutScroll.from(300);
            contextWithoutScroll.close();

            // resultWindow greater than maxResultWindow and scrollContext is null
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> contextWithoutScroll.preProcess());
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Result window is too large, from + size must be less than or equal to:"
                        + " ["
                        + maxResultWindow
                        + "] but was [310]. See the scroll api for a more efficient way to request large data sets. "
                        + "This limit can be set by changing the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            // resultWindow greater than maxResultWindow and scrollContext isn't null
            when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMillis(randomInt(1000))));
            ReaderContext readerContext = new LegacyReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                shardSearchRequest,
                randomNonNegativeLong()
            );
            DefaultSearchContext context1 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, timeout, null, false);
            context1.from(300);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess());
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Batch size is too large, size must be less than or equal to: ["
                        + maxResultWindow
                        + "] but was [310]. Scroll batch sizes cost as much memory as result windows so they are "
                        + "controlled by the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            // resultWindow not greater than maxResultWindow and both rescore and sort are not null
            context1.from(0);
            DocValueFormat docValueFormat = mock(DocValueFormat.class);
            SortAndFormats sortAndFormats = new SortAndFormats(new Sort(), new DocValueFormat[] { docValueFormat });
            context1.sort(sortAndFormats);

            RescoreContext rescoreContext = mock(RescoreContext.class);
            when(rescoreContext.getWindowSize()).thenReturn(500);
            context1.addRescore(rescoreContext);

            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess());
            assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

            // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
            context1.sort(null);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess());

            assertThat(
                exception.getMessage(),
                equalTo(
                    "Rescore window ["
                        + rescoreContext.getWindowSize()
                        + "] is too large. "
                        + "It must be less than ["
                        + maxRescoreWindow
                        + "]. This prevents allocating massive heaps for storing the results "
                        + "to be rescored. This limit can be set by changing the ["
                        + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                        + "] index level setting."
                )
            );

            readerContext.close();
            readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            ) {
                @Override
                public ScrollContext scrollContext() {
                    ScrollContext scrollContext = new ScrollContext();
                    scrollContext.scroll = new Scroll(TimeValue.timeValueSeconds(5));
                    return scrollContext;
                }
            };
            // rescore is null but sliceBuilder is not null
            DefaultSearchContext context2 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, timeout, null, false);

            SliceBuilder sliceBuilder = mock(SliceBuilder.class);
            int numSlices = maxSlicesPerScroll + randomIntBetween(1, 100);
            when(sliceBuilder.getMax()).thenReturn(numSlices);
            context2.sliceBuilder(sliceBuilder);

            exception = expectThrows(IllegalArgumentException.class, () -> context2.preProcess());
            assertThat(
                exception.getMessage(),
                equalTo(
                    "The number of slices ["
                        + numSlices
                        + "] is too large. It must "
                        + "be less than ["
                        + maxSlicesPerScroll
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_SCROLL.getKey()
                        + "] index level setting."
                )
            );

            // No exceptions should be thrown
            when(shardSearchRequest.getAliasFilter()).thenReturn(AliasFilter.EMPTY);
            when(shardSearchRequest.indexBoost()).thenReturn(AbstractQueryBuilder.DEFAULT_BOOST);

            DefaultSearchContext context3 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, timeout, null, false);
            ParsedQuery parsedQuery = ParsedQuery.parsedMatchAllQuery();
            context3.sliceBuilder(null).parsedQuery(parsedQuery).preProcess();
            assertEquals(context3.query(), context3.buildFilteredQuery(parsedQuery.query()));

            when(searchExecutionContext.getIndexSettings()).thenReturn(indexSettings);
            when(searchExecutionContext.getFieldType(anyString())).thenReturn(mock(MappedFieldType.class));

            readerContext.close();
            readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            DefaultSearchContext context4 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, timeout, null, false);
            context4.sliceBuilder(new SliceBuilder(1, 2)).parsedQuery(parsedQuery).preProcess();
            Query query1 = context4.query();
            context4.sliceBuilder(new SliceBuilder(0, 2)).parsedQuery(parsedQuery).preProcess();
            Query query2 = context4.query();
            assertTrue(query1 instanceof MatchNoDocsQuery || query2 instanceof MatchNoDocsQuery);

            readerContext.close();
            threadPool.shutdown();
        }
    }

    public void testClearQueryCancellationsOnClose() throws IOException {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        IndexService indexService = mock(IndexService.class);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            final Engine.SearcherSupplier searcherSupplier = new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {}

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher(
                            "test",
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(),
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            reader
                        );
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };
            SearchShardTarget target = new SearchShardTarget("node", shardId, null);
            ReaderContext readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier,
                randomNonNegativeLong(),
                false
            );
            DefaultSearchContext context = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, timeout, null, false);

            assertThat(context.searcher().hasCancellations(), is(false));
            context.searcher().addQueryCancellation(() -> {});
            assertThat(context.searcher().hasCancellations(), is(true));

            context.close();
            assertThat(context.searcher().hasCancellations(), is(false));

            readerContext.close();
        } finally {
            threadPool.shutdown();
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}
