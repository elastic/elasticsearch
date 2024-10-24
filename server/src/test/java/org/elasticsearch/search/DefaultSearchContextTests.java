/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.BinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSearchContextTests extends MapperServiceTestCase {

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
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        int maxSlicesPerScroll = randomIntBetween(50, 100);
        Settings settings = indexSettings(IndexVersion.current(), 2, 1).put("index.max_result_window", maxResultWindow)
            .put("index.max_slices_per_scroll", maxSlicesPerScroll)
            .put("index.max_rescore_window", maxRescoreWindow)
            .build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(indexService.newSearchExecutionContext(eq(shardId.id()), eq(shardId.id()), any(), any(), nullable(String.class), any(), any()))
            .thenReturn(searchExecutionContext);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(randomBoolean());
        when(indexService.mapperService()).thenReturn(mapperService);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(searchExecutionContext.getIndexSettings()).thenReturn(indexSettings);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(indexSettings.getIndexVersionCreated());

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
                false,
                null,
                randomFrom(SearchService.ResultsType.values()),
                randomBoolean(),
                randomInt()
            );
            contextWithoutScroll.from(300);
            contextWithoutScroll.close();

            // resultWindow greater than maxResultWindow and scrollContext is null
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, contextWithoutScroll::preProcess);
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
            try (
                DefaultSearchContext context1 = new DefaultSearchContext(
                    readerContext,
                    shardSearchRequest,
                    target,
                    null,
                    timeout,
                    null,
                    false,
                    null,
                    randomFrom(SearchService.ResultsType.values()),
                    randomBoolean(),
                    randomInt()
                )
            ) {
                context1.from(300);
                exception = expectThrows(IllegalArgumentException.class, context1::preProcess);
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

                exception = expectThrows(IllegalArgumentException.class, context1::preProcess);
                assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

                // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
                context1.sort(null);
                exception = expectThrows(IllegalArgumentException.class, context1::preProcess);

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
            }

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
            try (
                DefaultSearchContext context2 = new DefaultSearchContext(
                    readerContext,
                    shardSearchRequest,
                    target,
                    null,
                    timeout,
                    null,
                    false,
                    null,
                    randomFrom(SearchService.ResultsType.values()),
                    randomBoolean(),
                    randomInt()
                )
            ) {

                SliceBuilder sliceBuilder = mock(SliceBuilder.class);
                int numSlices = maxSlicesPerScroll + randomIntBetween(1, 100);
                when(sliceBuilder.getMax()).thenReturn(numSlices);
                context2.sliceBuilder(sliceBuilder);

                exception = expectThrows(IllegalArgumentException.class, context2::preProcess);
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
            }

            ParsedQuery parsedQuery = ParsedQuery.parsedMatchAllQuery();
            try (
                DefaultSearchContext context3 = new DefaultSearchContext(
                    readerContext,
                    shardSearchRequest,
                    target,
                    null,
                    timeout,
                    null,
                    false,
                    null,
                    randomFrom(SearchService.ResultsType.values()),
                    randomBoolean(),
                    randomInt()
                )
            ) {
                context3.sliceBuilder(null).parsedQuery(parsedQuery).preProcess();
                assertEquals(context3.query(), context3.buildFilteredQuery(parsedQuery.query()));

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
            }

            try (
                DefaultSearchContext context4 = new DefaultSearchContext(
                    readerContext,
                    shardSearchRequest,
                    target,
                    null,
                    timeout,
                    null,
                    false,
                    null,
                    randomFrom(SearchService.ResultsType.values()),
                    randomBoolean(),
                    randomInt()
                )
            ) {
                context4.sliceBuilder(new SliceBuilder(1, 2)).parsedQuery(parsedQuery).preProcess();
                Query query1 = context4.query();
                context4.sliceBuilder(new SliceBuilder(0, 2)).parsedQuery(parsedQuery).preProcess();
                Query query2 = context4.query();
                assertTrue(query1 instanceof MatchNoDocsQuery || query2 instanceof MatchNoDocsQuery);

                readerContext.close();
                threadPool.shutdown();
            }
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
            DefaultSearchContext context = new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                timeout,
                null,
                false,
                null,
                randomFrom(SearchService.ResultsType.values()),
                randomBoolean(),
                randomInt()
            );

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

    public void testNewIdLoader() throws Exception {
        try (DefaultSearchContext context = createDefaultSearchContext(Settings.EMPTY)) {
            assertThat(context.newIdLoader(), instanceOf(IdLoader.StoredIdLoader.class));
            context.indexShard().getThreadPool().shutdown();
        }
    }

    public void testNewIdLoaderWithTsdb() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2001-01-01T00:00:00.000Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
            .build();
        try (DefaultSearchContext context = createDefaultSearchContext(settings)) {
            assertThat(context.newIdLoader(), instanceOf(IdLoader.TsIdLoader.class));
            context.indexShard().getThreadPool().shutdown();
        }
    }

    public void testNewIdLoaderWithTsdbAndRoutingPathMatch() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2001-01-01T00:00:00.000Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "labels.*")
            .build();

        XContentBuilder mappings = mapping(b -> {
            b.startObject("labels").field("type", "object");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.endObject();
            }
            b.endObject();
        });

        try (DefaultSearchContext context = createDefaultSearchContext(settings, mappings)) {
            assertThat(context.newIdLoader(), instanceOf(IdLoader.TsIdLoader.class));
            context.indexShard().getThreadPool().shutdown();
        }
    }

    private static ShardSearchRequest createParallelRequest() {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("index", "uuid", 0));
        return new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(randomBoolean()),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null
        );

    }

    public void testDetermineMaximumNumberOfSlicesNoExecutor() {
        ToLongFunction<String> fieldCardinality = name -> { throw new UnsupportedOperationException(); };
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                null,
                createParallelRequest(),
                SearchService.ResultsType.DFS,
                randomBoolean(),
                fieldCardinality
            )
        );
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                null,
                createParallelRequest(),
                SearchService.ResultsType.QUERY,
                randomBoolean(),
                fieldCardinality
            )
        );
    }

    public void testDetermineMaximumNumberOfSlicesNotThreadPoolExecutor() {
        ExecutorService notThreadPoolExecutor = Executors.newWorkStealingPool();
        ToLongFunction<String> fieldCardinality = name -> { throw new UnsupportedOperationException(); };
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                notThreadPoolExecutor,
                createParallelRequest(),
                SearchService.ResultsType.DFS,
                randomBoolean(),
                fieldCardinality
            )
        );
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                notThreadPoolExecutor,
                createParallelRequest(),
                SearchService.ResultsType.QUERY,
                randomBoolean(),
                fieldCardinality
            )
        );
    }

    public void testDetermineMaximumNumberOfSlicesEnableQueryPhaseParallelCollection() {
        int executorPoolSize = randomIntBetween(1, 100);
        ThreadPoolExecutor threadPoolExecutor = EsExecutors.newFixed(
            "test",
            executorPoolSize,
            0,
            Thread::new,
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        ToLongFunction<String> fieldCardinality = name -> -1;
        assertEquals(
            executorPoolSize,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                threadPoolExecutor,
                createParallelRequest(),
                SearchService.ResultsType.QUERY,
                true,
                fieldCardinality
            )
        );
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                threadPoolExecutor,
                createParallelRequest(),
                SearchService.ResultsType.QUERY,
                false,
                fieldCardinality
            )
        );
        assertEquals(
            executorPoolSize,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                threadPoolExecutor,
                createParallelRequest(),
                SearchService.ResultsType.DFS,
                randomBoolean(),
                fieldCardinality
            )
        );
    }

    public void testDetermineMaximumNumberOfSlicesSingleSortByField() {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("index", "uuid", 0));
        ShardSearchRequest singleSliceReq = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(randomBoolean())
                .source(new SearchSourceBuilder().sort(SortBuilders.fieldSort(FieldSortBuilder.DOC_FIELD_NAME))),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null
        );
        ToLongFunction<String> fieldCardinality = name -> { throw new UnsupportedOperationException(); };
        int executorPoolSize = randomIntBetween(1, 100);
        ThreadPoolExecutor threadPoolExecutor = EsExecutors.newFixed(
            "test",
            executorPoolSize,
            0,
            Thread::new,
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        // DFS concurrency does not rely on slices, hence it kicks in regardless of the request (supportsParallelCollection is not called)
        assertEquals(
            executorPoolSize,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                threadPoolExecutor,
                singleSliceReq,
                SearchService.ResultsType.DFS,
                true,
                fieldCardinality
            )
        );
        assertEquals(
            1,
            DefaultSearchContext.determineMaximumNumberOfSlices(
                threadPoolExecutor,
                singleSliceReq,
                SearchService.ResultsType.QUERY,
                true,
                fieldCardinality
            )
        );
    }

    public void testDetermineMaximumNumberOfSlicesWithQueue() {
        int executorPoolSize = randomIntBetween(1, 100);
        ThreadPoolExecutor threadPoolExecutor = EsExecutors.newFixed(
            "test",
            executorPoolSize,
            1000,
            Thread::new,
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        ToLongFunction<String> fieldCardinality = name -> { throw new UnsupportedOperationException(); };

        for (int i = 0; i < executorPoolSize; i++) {
            assertTrue(threadPoolExecutor.getQueue().offer(() -> {}));
            assertEquals(
                executorPoolSize,
                DefaultSearchContext.determineMaximumNumberOfSlices(
                    threadPoolExecutor,
                    createParallelRequest(),
                    SearchService.ResultsType.DFS,
                    true,
                    fieldCardinality
                )
            );
            assertEquals(
                executorPoolSize,
                DefaultSearchContext.determineMaximumNumberOfSlices(
                    threadPoolExecutor,
                    createParallelRequest(),
                    SearchService.ResultsType.QUERY,
                    true,
                    fieldCardinality
                )
            );
        }
        for (int i = 0; i < 100; i++) {
            assertTrue(threadPoolExecutor.getQueue().offer(() -> {}));
            assertEquals(
                1,
                DefaultSearchContext.determineMaximumNumberOfSlices(
                    threadPoolExecutor,
                    createParallelRequest(),
                    SearchService.ResultsType.DFS,
                    true,
                    fieldCardinality
                )
            );
            assertEquals(
                1,
                DefaultSearchContext.determineMaximumNumberOfSlices(
                    threadPoolExecutor,
                    createParallelRequest(),
                    SearchService.ResultsType.QUERY,
                    true,
                    fieldCardinality
                )
            );
        }
    }

    public void testIsParallelCollectionSupportedForResults() {
        SearchSourceBuilder searchSourceBuilderOrNull = randomBoolean() ? null : new SearchSourceBuilder();
        ToLongFunction<String> fieldCardinality = name -> -1;
        for (var resultsType : SearchService.ResultsType.values()) {
            switch (resultsType) {
                case NONE, RANK_FEATURE, FETCH -> assertFalse(
                    "NONE, RANK_FEATURE, and FETCH phases do not support parallel collection.",
                    DefaultSearchContext.isParallelCollectionSupportedForResults(
                        resultsType,
                        searchSourceBuilderOrNull,
                        fieldCardinality,
                        randomBoolean()
                    )
                );
                case DFS -> assertTrue(
                    "DFS phase always supports parallel collection.",
                    DefaultSearchContext.isParallelCollectionSupportedForResults(
                        resultsType,
                        searchSourceBuilderOrNull,
                        fieldCardinality,
                        randomBoolean()
                    )
                );
                case QUERY -> {
                    SearchSourceBuilder searchSourceBuilderNoAgg = new SearchSourceBuilder();
                    assertTrue(
                        "Parallel collection should be supported for the query phase when no agg is present.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(
                            resultsType,
                            searchSourceBuilderNoAgg,
                            fieldCardinality,
                            true
                        )
                    );
                    assertTrue(
                        "Parallel collection should be supported for the query phase when the source is null.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(resultsType, null, fieldCardinality, true)
                    );

                    SearchSourceBuilder searchSourceAggSupportsParallelCollection = new SearchSourceBuilder();
                    searchSourceAggSupportsParallelCollection.aggregation(new DateRangeAggregationBuilder("dateRange"));
                    assertTrue(
                        "Parallel collection should be supported for the query phase when when enabled && contains supported agg.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(
                            resultsType,
                            searchSourceAggSupportsParallelCollection,
                            fieldCardinality,
                            true
                        )
                    );

                    assertFalse(
                        "Parallel collection should not be supported for the query phase when disabled.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(
                            resultsType,
                            searchSourceBuilderNoAgg,
                            fieldCardinality,
                            false
                        )
                    );
                    assertFalse(
                        "Parallel collection should not be supported for the query phase when disabled and source is null.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(resultsType, null, fieldCardinality, false)
                    );

                    SearchSourceBuilder searchSourceAggDoesNotSupportParallelCollection = new SearchSourceBuilder();
                    searchSourceAggDoesNotSupportParallelCollection.aggregation(new TermsAggregationBuilder("terms"));
                    assertFalse(
                        "Parallel collection should not be supported for the query phase when "
                            + "enabled && does not contains supported agg.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(
                            resultsType,
                            searchSourceAggDoesNotSupportParallelCollection,
                            fieldCardinality,
                            true
                        )
                    );

                    SearchSourceBuilder searchSourceMultiAggDoesNotSupportParallelCollection = new SearchSourceBuilder();
                    searchSourceMultiAggDoesNotSupportParallelCollection.aggregation(new TermsAggregationBuilder("terms"));
                    searchSourceMultiAggDoesNotSupportParallelCollection.aggregation(new DateRangeAggregationBuilder("dateRange"));
                    assertFalse(
                        "Parallel collection should not be supported for the query phase when when enabled && contains unsupported agg.",
                        DefaultSearchContext.isParallelCollectionSupportedForResults(
                            resultsType,
                            searchSourceMultiAggDoesNotSupportParallelCollection,
                            fieldCardinality,
                            true
                        )
                    );
                }
                default -> throw new UnsupportedOperationException("Untested ResultsType added, please add new testcases.");
            }
        }
    }

    public void testGetFieldCardinalityNoLeaves() throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
            writer.close();
            try (DirectoryReader reader = DirectoryReader.open(writer.getDirectory())) {
                SortedOrdinalsIndexFieldData high = new SortedOrdinalsIndexFieldData(
                    new IndexFieldDataCache.None(),
                    "empty",
                    CoreValuesSourceType.KEYWORD,
                    new NoneCircuitBreakerService(),
                    null
                );
                assertEquals(0, DefaultSearchContext.getFieldCardinality(high, reader));
            }
        }
    }

    public void testGetFieldCardinalityNoLeavesNoGlobalOrdinals() throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
            writer.close();
            try (DirectoryReader reader = DirectoryReader.open(writer.getDirectory())) {
                BinaryIndexFieldData binaryIndexFieldData = new BinaryIndexFieldData("high", CoreValuesSourceType.KEYWORD);
                assertEquals(-1, DefaultSearchContext.getFieldCardinality(binaryIndexFieldData, reader));
            }
        }
    }

    public void testGetFieldCardinality() throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory()) {
            final int numDocs = scaledRandomIntBetween(100, 200);
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; ++i) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("high", new BytesRef(Integer.toString(i))));
                    doc.add(new SortedSetDocValuesField("low", new BytesRef(Integer.toString(i % 3))));
                    w.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                BinaryIndexFieldData binaryIndexFieldData = new BinaryIndexFieldData("high", CoreValuesSourceType.KEYWORD);
                assertEquals(-1, DefaultSearchContext.getFieldCardinality(binaryIndexFieldData, reader));
                SortedOrdinalsIndexFieldData nonExistent = new SortedOrdinalsIndexFieldData(
                    new IndexFieldDataCache.None(),
                    "non_existent",
                    CoreValuesSourceType.KEYWORD,
                    new NoneCircuitBreakerService(),
                    null
                );
                assertEquals(0, DefaultSearchContext.getFieldCardinality(nonExistent, reader));
                SortedOrdinalsIndexFieldData high = new SortedOrdinalsIndexFieldData(
                    new IndexFieldDataCache.None(),
                    "high",
                    CoreValuesSourceType.KEYWORD,
                    new NoneCircuitBreakerService(),
                    null
                );
                assertEquals(numDocs, DefaultSearchContext.getFieldCardinality(high, reader));
                SortedOrdinalsIndexFieldData low = new SortedOrdinalsIndexFieldData(
                    new IndexFieldDataCache.None(),
                    "low",
                    CoreValuesSourceType.KEYWORD,
                    new NoneCircuitBreakerService(),
                    null
                );
                assertEquals(3, DefaultSearchContext.getFieldCardinality(low, reader));
            }
        }
    }

    public void testGetFieldCardinalityNumeric() throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory()) {
            final int numDocs = scaledRandomIntBetween(100, 200);
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; ++i) {
                    Document doc = new Document();
                    doc.add(new LongField("long", i, Field.Store.NO));
                    doc.add(new IntField("int", i, Field.Store.NO));
                    doc.add(new SortedNumericDocValuesField("no_index", i));
                    w.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final SortedNumericIndexFieldData longFieldData = new SortedNumericIndexFieldData(
                    "long",
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null,
                    true
                );
                assertEquals(numDocs, DefaultSearchContext.getFieldCardinality(longFieldData, reader));

                final SortedNumericIndexFieldData integerFieldData = new SortedNumericIndexFieldData(
                    "int",
                    IndexNumericFieldData.NumericType.INT,
                    IndexNumericFieldData.NumericType.INT.getValuesSourceType(),
                    null,
                    true
                );
                assertEquals(numDocs, DefaultSearchContext.getFieldCardinality(integerFieldData, reader));

                final SortedNumericIndexFieldData shortFieldData = new SortedNumericIndexFieldData(
                    "int",
                    IndexNumericFieldData.NumericType.SHORT,
                    IndexNumericFieldData.NumericType.SHORT.getValuesSourceType(),
                    null,
                    true
                );
                assertEquals(numDocs, DefaultSearchContext.getFieldCardinality(shortFieldData, reader));

                final SortedNumericIndexFieldData noIndexFieldata = new SortedNumericIndexFieldData(
                    "no_index",
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    null,
                    false
                );
                assertEquals(-1, DefaultSearchContext.getFieldCardinality(noIndexFieldata, reader));
            }
        }
    }

    public void testGetFieldCardinalityUnmappedField() {
        MapperService mapperService = mock(MapperService.class);
        IndexService indexService = mock(IndexService.class);
        when(indexService.mapperService()).thenReturn(mapperService);
        assertEquals(-1, DefaultSearchContext.getFieldCardinality("field", indexService, null));
    }

    public void testGetFieldCardinalityRuntimeField() {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(anyString())).thenReturn(new MockFieldMapper.FakeFieldType("field"));
        IndexService indexService = mock(IndexService.class);
        when(indexService.mapperService()).thenReturn(mapperService);
        when(indexService.loadFielddata(any(), any())).thenThrow(new RuntimeException());
        assertEquals(-1, DefaultSearchContext.getFieldCardinality("field", indexService, null));
    }

    private DefaultSearchContext createDefaultSearchContext(Settings providedIndexSettings) throws IOException {
        return createDefaultSearchContext(providedIndexSettings, null);
    }

    private DefaultSearchContext createDefaultSearchContext(Settings providedIndexSettings, XContentBuilder mappings) throws IOException {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);
        when(shardSearchRequest.shardRequestIndex()).thenReturn(shardId.id());
        when(shardSearchRequest.numberOfShards()).thenReturn(2);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        Settings settings = indexSettings(IndexVersion.current(), 2, 1).put(providedIndexSettings).build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(indexService.newSearchExecutionContext(eq(shardId.id()), eq(shardId.id()), any(), any(), nullable(String.class), any()))
            .thenReturn(searchExecutionContext);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings;
        MapperService mapperService;
        if (mappings != null) {
            mapperService = createMapperService(settings, mappings);
            indexSettings = mapperService.getIndexSettings();
        } else {
            indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
            mapperService = mock(MapperService.class);
            when(mapperService.hasNested()).thenReturn(randomBoolean());
            when(indexService.mapperService()).thenReturn(mapperService);
            when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        }
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.mapperService()).thenReturn(mapperService);
        when(indexService.getMetadata()).thenReturn(indexMetadata);
        when(searchExecutionContext.getIndexSettings()).thenReturn(indexSettings);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(indexSettings.getIndexVersionCreated());

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

            ReaderContext readerContext = new ReaderContext(
                newContextId(),
                indexService,
                indexShard,
                searcherSupplier.get(),
                randomNonNegativeLong(),
                false
            );
            return new DefaultSearchContext(
                readerContext,
                shardSearchRequest,
                target,
                null,
                timeout,
                null,
                false,
                null,
                randomFrom(SearchService.ResultsType.values()),
                randomBoolean(),
                randomInt()
            );
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}
