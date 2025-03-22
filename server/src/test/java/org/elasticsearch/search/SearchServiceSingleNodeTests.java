/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService.ResultsType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.NonCountingTermQuery;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rank.TestRankShardResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.search.SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED;
import static org.elasticsearch.search.SearchService.SEARCH_WORKER_THREADS_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class SearchServiceSingleNodeTests extends ESSingleNodeTestCase {

    private static final int SEARCH_POOL_SIZE = 10;

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            FailOnRewriteQueryPlugin.class,
            CustomScriptPlugin.class,
            ReaderWrapperCountPlugin.class,
            InternalOrPrivateSettingsPlugin.class,
            MockSearchService.TestPlugin.class
        );
    }

    public static class ReaderWrapperCountPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(service -> SearchServiceSingleNodeTests::apply);
        }
    }

    @Before
    public void resetCount() {
        numWrapInvocations = new AtomicInteger(0);
    }

    private static AtomicInteger numWrapInvocations = new AtomicInteger(0);

    private static DirectoryReader apply(DirectoryReader directoryReader) throws IOException {
        numWrapInvocations.incrementAndGet();
        return new FilterDirectoryReader(directoryReader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return reader;
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return directoryReader.getReaderCacheHelper();
            }
        };
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        static final String DUMMY_SCRIPT = "dummyScript";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(DUMMY_SCRIPT, vars -> "dummy");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onFetchPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search]"));
                    }
                }

                @Override
                public void onQueryPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search]"));
                    }
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put("search.default_search_timeout", "5s")
            .put("thread_pool.search.size", SEARCH_POOL_SIZE) // customized search pool size, reconfiguring at runtime is unsupported
            .build();
    }

    public void testClearOnClose() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertResponse(
            client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)),
            searchResponse -> assertThat(searchResponse.getScrollId(), is(notNullValue()))
        );
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertResponse(
            client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)),
            searchResponse -> assertThat(searchResponse.getScrollId(), is(notNullValue()))
        );
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertResponse(
            client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)),
            searchResponse -> assertThat(searchResponse.getScrollId(), is(notNullValue()))
        );
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(indicesAdmin().prepareDelete("index"));
        awaitIndexShardCloseAsyncTasks();
        assertEquals(0, service.getActiveContexts());
    }

    public void testCloseSearchContextOnRewriteException() {
        // if refresh happens while checking the exception, the subsequent reference count might not match, so we switch it off
        createIndex("index", Settings.builder().put("index.refresh_interval", -1).build());
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        final int activeContexts = service.getActiveContexts();
        final int activeRefs = indexShard.store().refCount();
        expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("index").setQuery(new FailOnRewriteQueryBuilder()).get()
        );
        assertEquals(activeContexts, service.getActiveContexts());
        assertEquals(activeRefs, indexShard.store().refCount());
    }

    public void testSearchWhileIndexDeleted() throws InterruptedException {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startGun = new CountDownLatch(1);
        final int permitCount = 100;
        Semaphore semaphore = new Semaphore(permitCount);
        ShardRouting routing = TestShardRouting.newShardRouting(
            indexShard.shardId(),
            randomAlphaOfLength(5),
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        final Thread thread = new Thread(() -> {
            startGun.countDown();
            while (running.get()) {
                if (randomBoolean()) {
                    service.afterIndexRemoved(indexService.index(), indexService.getIndexSettings(), DELETED);
                } else {
                    service.beforeIndexShardCreated(routing, indexService.getIndexSettings().getSettings());
                }
                if (randomBoolean()) {
                    // here we trigger some refreshes to ensure the IR go out of scope such that we hit ACE if we access a search
                    // context in a non-sane way.
                    try {
                        semaphore.acquire();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    prepareIndex("index").setSource("field", "value")
                        .setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()))
                        .execute(ActionListener.running(semaphore::release));
                }
            }
        });
        thread.start();
        startGun.await();
        try {
            final int rounds = scaledRandomIntBetween(100, 10000);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true).scroll(TimeValue.timeValueMinutes(1));
            for (int i = 0; i < rounds; i++) {
                try {
                    try {
                        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
                        final boolean useScroll = randomBoolean();
                        service.executeQueryPhase(
                            new ShardSearchRequest(
                                OriginalIndices.NONE,
                                useScroll ? scrollSearchRequest : searchRequest,
                                indexShard.shardId(),
                                0,
                                1,
                                AliasFilter.EMPTY,
                                1.0f,
                                -1,
                                null
                            ),
                            new SearchShardTask(123L, "", "", "", null, emptyMap()),
                            result.delegateFailure((l, r) -> {
                                r.incRef();
                                l.onResponse(r);
                            })
                        );
                        final SearchPhaseResult searchPhaseResult = result.get();
                        try {
                            List<Integer> intCursors = new ArrayList<>(1);
                            intCursors.add(0);
                            ShardFetchRequest req = new ShardFetchRequest(
                                searchPhaseResult.getContextId(),
                                intCursors,
                                null/* not a scroll */
                            );
                            PlainActionFuture<FetchSearchResult> listener = new PlainActionFuture<>();
                            service.executeFetchPhase(req, new SearchShardTask(123L, "", "", "", null, emptyMap()), listener);
                            listener.get();
                            if (useScroll) {
                                // have to free context since this test does not remove the index from IndicesService.
                                service.freeReaderContext(searchPhaseResult.getContextId());
                            }
                        } finally {
                            searchPhaseResult.decRef();
                        }
                    } catch (ExecutionException ex) {
                        assertThat(ex.getCause(), instanceOf(RuntimeException.class));
                        throw ((RuntimeException) ex.getCause());
                    }
                } catch (AlreadyClosedException ex) {
                    throw ex;
                } catch (IllegalStateException ex) {
                    assertEquals(AbstractRefCounted.ALREADY_CLOSED_MESSAGE, ex.getMessage());
                } catch (SearchContextMissingException ex) {
                    // that's fine
                }
            }
        } finally {
            running.set(false);
            thread.join();
            semaphore.acquire(permitCount);
        }

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testRankFeaturePhaseSearchPhases() throws InterruptedException, ExecutionException {
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 3;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName);
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        final SearchService service = getInstanceFromNode(SearchService.class);

        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        final IndexShard indexShard = indexService.getShard(0);
        SearchShardTask searchTask = new SearchShardTask(123L, "", "", "", null, emptyMap());

        // create a SearchRequest that will return all documents and defines a TestRankBuilder with shard-level only operations
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true)
            .source(
                new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                    .size(DEFAULT_SIZE)
                    .fetchField(fetchFieldName)
                    .rankBuilder(
                        // here we override only the shard-level contexts
                        new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                            @Override
                            public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                                return new QueryPhaseRankShardContext(queries, from) {

                                    @Override
                                    public int rankWindowSize() {
                                        return DEFAULT_RANK_WINDOW_SIZE;
                                    }

                                    @Override
                                    public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                        // we know we have just 1 query, so return all the docs from it
                                        return new TestRankShardResult(
                                            Arrays.stream(rankResults.get(0).scoreDocs)
                                                .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                                .limit(rankWindowSize())
                                                .toArray(RankDoc[]::new)
                                        );
                                    }
                                };
                            }

                            @Override
                            public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                                return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                    @Override
                                    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                                        for (int i = 0; i < hits.getHits().length; i++) {
                                            SearchHit hit = hits.getHits()[i];
                                            rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                                            rankFeatureDocs[i].featureData(hit.getFields().get(rankFeatureFieldName).getValue());
                                            rankFeatureDocs[i].score = (numDocs - i) + randomFloat();
                                            rankFeatureDocs[i].rank = i + 1;
                                        }
                                        return new RankFeatureShardResult(rankFeatureDocs);
                                    }
                                };
                            }
                        }
                    )
            );

        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        QuerySearchResult queryResult = null;
        RankFeatureResult rankResult = null;
        try {
            // Execute the query phase and store the result in a SearchPhaseResult container using a PlainActionFuture
            PlainActionFuture<SearchPhaseResult> queryPhaseResults = new PlainActionFuture<>();
            service.executeQueryPhase(request, searchTask, queryPhaseResults);
            queryResult = (QuerySearchResult) queryPhaseResults.get();

            // these are the matched docs from the query phase
            final RankDoc[] queryRankDocs = ((TestRankShardResult) queryResult.getRankShardResult()).testRankDocs;

            // assume that we have cut down to these from the coordinator node as the top-docs to run the rank feature phase upon
            List<Integer> topRankWindowSizeDocs = randomNonEmptySubsetOf(Arrays.stream(queryRankDocs).map(x -> x.doc).toList());

            // now we create a RankFeatureShardRequest to extract feature info for the top-docs above
            RankFeatureShardRequest rankFeatureShardRequest = new RankFeatureShardRequest(
                OriginalIndices.NONE,
                queryResult.getContextId(), // use the context from the query phase
                request,
                topRankWindowSizeDocs
            );
            PlainActionFuture<RankFeatureResult> rankPhaseResults = new PlainActionFuture<>();
            service.executeRankFeaturePhase(rankFeatureShardRequest, searchTask, rankPhaseResults);
            rankResult = rankPhaseResults.get();

            assertNotNull(rankResult);
            assertNotNull(rankResult.rankFeatureResult());
            RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
            assertNotNull(rankFeatureShardResult);

            List<Integer> sortedRankWindowDocs = topRankWindowSizeDocs.stream().sorted().toList();
            assertEquals(sortedRankWindowDocs.size(), rankFeatureShardResult.rankFeatureDocs.length);
            for (int i = 0; i < sortedRankWindowDocs.size(); i++) {
                assertEquals((long) sortedRankWindowDocs.get(i), rankFeatureShardResult.rankFeatureDocs[i].doc);
                assertEquals(rankFeatureShardResult.rankFeatureDocs[i].featureData, "aardvark_" + sortedRankWindowDocs.get(i));
            }

            List<Integer> globalTopKResults = randomNonEmptySubsetOf(
                Arrays.stream(rankFeatureShardResult.rankFeatureDocs).map(x -> x.doc).toList()
            );

            // finally let's create a fetch request to bring back fetch info for the top results
            ShardFetchSearchRequest fetchRequest = new ShardFetchSearchRequest(
                OriginalIndices.NONE,
                rankResult.getContextId(),
                request,
                globalTopKResults,
                null,
                null,
                rankResult.getRescoreDocIds(),
                null
            );

            // execute fetch phase and perform any validations once we retrieve the response
            // the difference in how we do assertions here is needed because once the transport service sends back the response
            // it decrements the reference to the FetchSearchResult (through the ActionListener#respondAndRelease) and sets hits to null
            PlainActionFuture<FetchSearchResult> fetchListener = new PlainActionFuture<>() {
                @Override
                public void onResponse(FetchSearchResult fetchSearchResult) {
                    assertNotNull(fetchSearchResult);
                    assertNotNull(fetchSearchResult.hits());

                    int totalHits = fetchSearchResult.hits().getHits().length;
                    assertEquals(globalTopKResults.size(), totalHits);
                    for (int i = 0; i < totalHits; i++) {
                        // rank and score are set by the SearchPhaseController#merge so no need to validate that here
                        SearchHit hit = fetchSearchResult.hits().getAt(i);
                        assertNotNull(hit.getFields().get(fetchFieldName));
                        assertEquals(hit.getFields().get(fetchFieldName).getValue(), fetchFieldValue + "_" + hit.docId());
                    }
                    super.onResponse(fetchSearchResult);
                }

                @Override
                public void onFailure(Exception e) {
                    super.onFailure(e);
                    throw new AssertionError("No failure should have been raised", e);
                }
            };
            service.executeFetchPhase(fetchRequest, searchTask, fetchListener);
            fetchListener.get();
        } catch (Exception ex) {
            if (queryResult != null) {
                if (queryResult.hasReferences()) {
                    queryResult.decRef();
                }
                service.freeReaderContext(queryResult.getContextId());
            }
            if (rankResult != null && rankResult.hasReferences()) {
                rankResult.decRef();
            }
            throw ex;
        }
    }

    public void testRankFeaturePhaseUsingClient() {
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 4;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName);
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        ElasticsearchAssertions.assertResponse(
            client().prepareSearch(indexName)
                .setSource(
                    new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                        .size(2)
                        .from(2)
                        .fetchField(fetchFieldName)
                        .rankBuilder(
                            // here we override only the shard-level contexts
                            new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {

                                // no need for more than one queries
                                @Override
                                public boolean isCompoundBuilder() {
                                    return false;
                                }

                                @Override
                                public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(
                                    int size,
                                    int from,
                                    Client client
                                ) {
                                    return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE, false) {
                                        @Override
                                        protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                                            float[] scores = new float[featureDocs.length];
                                            for (int i = 0; i < featureDocs.length; i++) {
                                                scores[i] = featureDocs[i].score;
                                            }
                                            scoreListener.onResponse(scores);
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                                    return new QueryPhaseRankCoordinatorContext(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                                        @Override
                                        public ScoreDoc[] rankQueryPhaseResults(
                                            List<QuerySearchResult> querySearchResults,
                                            SearchPhaseController.TopDocsStats topDocStats
                                        ) {
                                            List<RankDoc> rankDocs = new ArrayList<>();
                                            for (int i = 0; i < querySearchResults.size(); i++) {
                                                QuerySearchResult querySearchResult = querySearchResults.get(i);
                                                TestRankShardResult shardResult = (TestRankShardResult) querySearchResult
                                                    .getRankShardResult();
                                                for (RankDoc trd : shardResult.testRankDocs) {
                                                    trd.shardIndex = i;
                                                    rankDocs.add(trd);
                                                }
                                            }
                                            rankDocs.sort(Comparator.comparing((RankDoc doc) -> doc.score).reversed());
                                            RankDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankDoc[]::new);
                                            topDocStats.fetchHits = topResults.length;
                                            return topResults;
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                                    return new QueryPhaseRankShardContext(queries, from) {

                                        @Override
                                        public int rankWindowSize() {
                                            return DEFAULT_RANK_WINDOW_SIZE;
                                        }

                                        @Override
                                        public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                            // we know we have just 1 query, so return all the docs from it
                                            return new TestRankShardResult(
                                                Arrays.stream(rankResults.get(0).scoreDocs)
                                                    .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                                    .limit(rankWindowSize())
                                                    .toArray(RankDoc[]::new)
                                            );
                                        }
                                    };
                                }

                                @Override
                                public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                                    return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                        @Override
                                        public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                            RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                                            for (int i = 0; i < hits.getHits().length; i++) {
                                                SearchHit hit = hits.getHits()[i];
                                                rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                                                rankFeatureDocs[i].featureData(hit.getFields().get(rankFeatureFieldName).getValue());
                                                rankFeatureDocs[i].score = randomFloat();
                                                rankFeatureDocs[i].rank = i + 1;
                                            }
                                            return new RankFeatureShardResult(rankFeatureDocs);
                                        }
                                    };
                                }
                            }
                        )
                ),
            (response) -> {
                SearchHits hits = response.getHits();
                assertEquals(hits.getTotalHits().value(), numDocs);
                assertEquals(hits.getHits().length, 2);
                int index = 0;
                for (SearchHit hit : hits.getHits()) {
                    assertEquals(hit.getRank(), 3 + index);
                    assertTrue(hit.getScore() >= 0);
                    assertEquals(hit.getFields().get(fetchFieldName).getValue(), fetchFieldValue + "_" + hit.docId());
                    index++;
                }
            }
        );
    }

    public void testRankFeaturePhaseExceptionOnCoordinatingNode() {
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 3;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName);
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(indexName)
                .setSource(
                    new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                        .size(2)
                        .from(2)
                        .fetchField(fetchFieldName)
                        .rankBuilder(new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {

                            // no need for more than one queries
                            @Override
                            public boolean isCompoundBuilder() {
                                return false;
                            }

                            @Override
                            public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(
                                int size,
                                int from,
                                Client client
                            ) {
                                return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE, false) {
                                    @Override
                                    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                                        throw new IllegalStateException("should have failed earlier");
                                    }
                                };
                            }

                            @Override
                            public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                                return new QueryPhaseRankCoordinatorContext(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                                    @Override
                                    public ScoreDoc[] rankQueryPhaseResults(
                                        List<QuerySearchResult> querySearchResults,
                                        SearchPhaseController.TopDocsStats topDocStats
                                    ) {
                                        throw new UnsupportedOperationException("simulated failure");
                                    }
                                };
                            }

                            @Override
                            public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                                return new QueryPhaseRankShardContext(queries, from) {

                                    @Override
                                    public int rankWindowSize() {
                                        return DEFAULT_RANK_WINDOW_SIZE;
                                    }

                                    @Override
                                    public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                        // we know we have just 1 query, so return all the docs from it
                                        return new TestRankShardResult(
                                            Arrays.stream(rankResults.get(0).scoreDocs)
                                                .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                                .limit(rankWindowSize())
                                                .toArray(RankDoc[]::new)
                                        );
                                    }
                                };
                            }

                            @Override
                            public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                                return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                    @Override
                                    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                                        for (int i = 0; i < hits.getHits().length; i++) {
                                            SearchHit hit = hits.getHits()[i];
                                            rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                                            rankFeatureDocs[i].featureData(hit.getFields().get(rankFeatureFieldName).getValue());
                                            rankFeatureDocs[i].score = randomFloat();
                                            rankFeatureDocs[i].rank = i + 1;
                                        }
                                        return new RankFeatureShardResult(rankFeatureDocs);
                                    }
                                };
                            }
                        })
                )
                .get()
        );
    }

    public void testRankFeaturePhaseExceptionAllShardFail() {
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 3;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName);
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(indexName)
                .setAllowPartialSearchResults(true)
                .setSource(
                    new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                        .fetchField(fetchFieldName)
                        .rankBuilder(
                            // here we override only the shard-level contexts
                            new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {

                                // no need for more than one queries
                                @Override
                                public boolean isCompoundBuilder() {
                                    return false;
                                }

                                @Override
                                public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(
                                    int size,
                                    int from,
                                    Client client
                                ) {
                                    return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE, false) {
                                        @Override
                                        protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                                            float[] scores = new float[featureDocs.length];
                                            for (int i = 0; i < featureDocs.length; i++) {
                                                scores[i] = featureDocs[i].score;
                                            }
                                            scoreListener.onResponse(scores);
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                                    return new QueryPhaseRankCoordinatorContext(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                                        @Override
                                        public ScoreDoc[] rankQueryPhaseResults(
                                            List<QuerySearchResult> querySearchResults,
                                            SearchPhaseController.TopDocsStats topDocStats
                                        ) {
                                            List<RankDoc> rankDocs = new ArrayList<>();
                                            for (int i = 0; i < querySearchResults.size(); i++) {
                                                QuerySearchResult querySearchResult = querySearchResults.get(i);
                                                TestRankShardResult shardResult = (TestRankShardResult) querySearchResult
                                                    .getRankShardResult();
                                                for (RankDoc trd : shardResult.testRankDocs) {
                                                    trd.shardIndex = i;
                                                    rankDocs.add(trd);
                                                }
                                            }
                                            rankDocs.sort(Comparator.comparing((RankDoc doc) -> doc.score).reversed());
                                            RankDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankDoc[]::new);
                                            topDocStats.fetchHits = topResults.length;
                                            return topResults;
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                                    return new QueryPhaseRankShardContext(queries, from) {

                                        @Override
                                        public int rankWindowSize() {
                                            return DEFAULT_RANK_WINDOW_SIZE;
                                        }

                                        @Override
                                        public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                            // we know we have just 1 query, so return all the docs from it
                                            return new TestRankShardResult(
                                                Arrays.stream(rankResults.get(0).scoreDocs)
                                                    .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                                    .limit(rankWindowSize())
                                                    .toArray(RankDoc[]::new)
                                            );
                                        }
                                    };
                                }

                                @Override
                                public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                                    return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                        @Override
                                        public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                            throw new UnsupportedOperationException("simulated failure");
                                        }
                                    };
                                }
                            }
                        )
                )
                .get()
        );
    }

    public void testRankFeaturePhaseExceptionOneShardFails() {
        // if we have only one shard and it fails, it will fallback to context.onPhaseFailure which will eventually clean up all contexts.
        // in this test we want to make sure that even if one shard (of many) fails during the RankFeaturePhase, then the appropriate
        // context will have been cleaned up.
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 3;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).build());
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        assertResponse(
            client().prepareSearch(indexName)
                .setAllowPartialSearchResults(true)
                .setSource(
                    new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                        .fetchField(fetchFieldName)
                        .rankBuilder(
                            // here we override only the shard-level contexts
                            new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {

                                // no need for more than one queries
                                @Override
                                public boolean isCompoundBuilder() {
                                    return false;
                                }

                                @Override
                                public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(
                                    int size,
                                    int from,
                                    Client client
                                ) {
                                    return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE, false) {
                                        @Override
                                        protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                                            float[] scores = new float[featureDocs.length];
                                            for (int i = 0; i < featureDocs.length; i++) {
                                                scores[i] = featureDocs[i].score;
                                            }
                                            scoreListener.onResponse(scores);
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                                    return new QueryPhaseRankCoordinatorContext(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                                        @Override
                                        public ScoreDoc[] rankQueryPhaseResults(
                                            List<QuerySearchResult> querySearchResults,
                                            SearchPhaseController.TopDocsStats topDocStats
                                        ) {
                                            List<RankDoc> rankDocs = new ArrayList<>();
                                            for (int i = 0; i < querySearchResults.size(); i++) {
                                                QuerySearchResult querySearchResult = querySearchResults.get(i);
                                                TestRankShardResult shardResult = (TestRankShardResult) querySearchResult
                                                    .getRankShardResult();
                                                for (RankDoc trd : shardResult.testRankDocs) {
                                                    trd.shardIndex = i;
                                                    rankDocs.add(trd);
                                                }
                                            }
                                            rankDocs.sort(Comparator.comparing((RankDoc doc) -> doc.score).reversed());
                                            RankDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankDoc[]::new);
                                            topDocStats.fetchHits = topResults.length;
                                            return topResults;
                                        }
                                    };
                                }

                                @Override
                                public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                                    return new QueryPhaseRankShardContext(queries, from) {

                                        @Override
                                        public int rankWindowSize() {
                                            return DEFAULT_RANK_WINDOW_SIZE;
                                        }

                                        @Override
                                        public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                            // we know we have just 1 query, so return all the docs from it
                                            return new TestRankShardResult(
                                                Arrays.stream(rankResults.get(0).scoreDocs)
                                                    .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                                    .limit(rankWindowSize())
                                                    .toArray(RankDoc[]::new)
                                            );
                                        }
                                    };
                                }

                                @Override
                                public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                                    return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                        @Override
                                        public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                            if (shardId == 0) {
                                                throw new UnsupportedOperationException("simulated failure");
                                            } else {
                                                RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                                                for (int i = 0; i < hits.getHits().length; i++) {
                                                    SearchHit hit = hits.getHits()[i];
                                                    rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                                                    rankFeatureDocs[i].featureData(hit.getFields().get(rankFeatureFieldName).getValue());
                                                    rankFeatureDocs[i].score = randomFloat();
                                                    rankFeatureDocs[i].rank = i + 1;
                                                }
                                                return new RankFeatureShardResult(rankFeatureDocs);
                                            }
                                        }
                                    };
                                }
                            }
                        )
                ),
            (searchResponse) -> {
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals("simulated failure", searchResponse.getShardFailures()[0].getCause().getMessage());
                assertNotEquals(0, searchResponse.getHits().getHits().length);
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertEquals(fetchFieldValue + "_" + hit.getId(), hit.getFields().get(fetchFieldName).getValue());
                    assertEquals(1, hit.getShard().getShardId().id());
                }
            }
        );
    }

    public void testSearchWhileIndexDeletedDoesNotLeakSearchContext() throws ExecutionException, InterruptedException {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        MockSearchService service = (MockSearchService) getInstanceFromNode(SearchService.class);
        service.setOnPutContext(context -> {
            if (context.indexShard() == indexShard) {
                assertAcked(indicesAdmin().prepareDelete("index"));
            }
        });

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true).scroll(TimeValue.timeValueMinutes(1));

        // the scrolls are not explicitly freed, but should all be gone when the test finished.
        // for completeness, we also randomly test the regular search path.
        final boolean useScroll = randomBoolean();
        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
        service.executeQueryPhase(
            new ShardSearchRequest(
                OriginalIndices.NONE,
                useScroll ? scrollSearchRequest : searchRequest,
                new ShardId(resolveIndex("index"), 0),
                0,
                1,
                AliasFilter.EMPTY,
                1.0f,
                -1,
                null
            ),
            new SearchShardTask(123L, "", "", "", null, emptyMap()),
            result
        );

        try {
            result.get();
        } catch (Exception e) {
            // ok
        }

        expectThrows(IndexNotFoundException.class, () -> indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("index").get());

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testBeforeShardLockDuringShardCreate() {
        IndexService indexService = createIndex("index", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertResponse(
            client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)),
            searchResponse -> assertThat(searchResponse.getScrollId(), is(notNullValue()))
        );
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.beforeIndexShardCreated(
            TestShardRouting.newShardRouting(
                "test",
                0,
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                randomBoolean(),
                ShardRoutingState.INITIALIZING
            ),
            indexService.getIndexSettings().getSettings()
        );
        assertEquals(1, service.getActiveContexts());

        service.beforeIndexShardCreated(
            TestShardRouting.newShardRouting(
                new ShardId(indexService.index(), 0),
                randomAlphaOfLength(5),
                randomBoolean(),
                ShardRoutingState.INITIALIZING
            ),
            indexService.getIndexSettings().getSettings()
        );
        assertEquals(0, service.getActiveContexts());
    }

    public void testTimeout() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ShardSearchRequest requestWithDefaultTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );

        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext contextWithDefaultTimeout = service.createContext(
                reader,
                requestWithDefaultTimeout,
                mock(SearchShardTask.class),
                ResultsType.NONE,
                randomBoolean()
            )
        ) {
            // the search context should inherit the default timeout
            assertThat(contextWithDefaultTimeout.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        }

        final long seconds = randomIntBetween(6, 10);
        searchRequest.source(new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(seconds)));
        final ShardSearchRequest requestWithCustomTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(
                reader,
                requestWithCustomTimeout,
                mock(SearchShardTask.class),
                ResultsType.NONE,
                randomBoolean()
            )
        ) {
            // the search context should inherit the query timeout
            assertThat(context.timeout(), equalTo(TimeValue.timeValueSeconds(seconds)));
        }
    }

    /**
     * test that getting more than the allowed number of docvalue_fields throws an exception
     */
    public void testMaxDocvalueFieldsSearch() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), 1).build();
        createIndex("index", settings, null, "field1", "keyword", "field2", "keyword");
        prepareIndex("index").setId("1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.docValueField("field1");

        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
        ) {
            assertNotNull(context);
        }

        searchSourceBuilder.docValueField("unmapped_field");
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
        ) {
            assertNotNull(context);
        }

        searchSourceBuilder.docValueField("field2");
        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
            );
            assertEquals(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [1] but was [2]. "
                    + "This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.",
                ex.getMessage()
            );
        }
    }

    public void testDeduplicateDocValuesFields() throws Exception {
        createIndex("index", Settings.EMPTY, "_doc", "field1", "type=date", "field2", "type=date");
        prepareIndex("index").setId("1").setSource("field1", "2022-08-03", "field2", "2022-08-04").setRefreshPolicy(IMMEDIATE).get();
        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.docValueField("f*");
            if (randomBoolean()) {
                searchSourceBuilder.docValueField("field*");
            }
            if (randomBoolean()) {
                searchSourceBuilder.docValueField("*2");
            }
            ShardSearchRequest request = new ShardSearchRequest(
                OriginalIndices.NONE,
                searchRequest,
                indexShard.shardId(),
                0,
                1,
                AliasFilter.EMPTY,
                1.0f,
                -1,
                null
            );
            try (
                SearchContext context = service.createContext(
                    reader,
                    request,
                    mock(SearchShardTask.class),
                    ResultsType.NONE,
                    randomBoolean()
                )
            ) {
                Collection<FieldAndFormat> fields = context.docValuesContext().fields();
                assertThat(fields, containsInAnyOrder(new FieldAndFormat("field1", null), new FieldAndFormat("field2", null)));
            }
        }
    }

    /**
     * test that getting more than the allowed number of script_fields throws an exception
     */
    public void testMaxScriptFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of script_fields to retrieve
        int maxScriptFields = indexService.getIndexSettings().getMaxScriptFields();
        for (int i = 0; i < maxScriptFields; i++) {
            searchSourceBuilder.scriptField(
                "field" + i,
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, emptyMap())
            );
        }
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );

        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            try (
                SearchContext context = service.createContext(
                    reader,
                    request,
                    mock(SearchShardTask.class),
                    ResultsType.NONE,
                    randomBoolean()
                )
            ) {
                assertNotNull(context);
            }
            searchSourceBuilder.scriptField(
                "anotherScriptField",
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, emptyMap())
            );
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
            );
            assertEquals(
                "Trying to retrieve too many script_fields. Must be less than or equal to: ["
                    + maxScriptFields
                    + "] but was ["
                    + (maxScriptFields + 1)
                    + "]. This limit can be set by changing the [index.max_script_fields] index level setting.",
                ex.getMessage()
            );
        }
    }

    public void testIgnoreScriptfieldIfSizeZero() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.scriptField(
            "field" + 0,
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, emptyMap())
        );
        searchSourceBuilder.size(0);
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        try (
            ReaderContext reader = createReaderContext(indexService, indexShard);
            SearchContext context = service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
        ) {
            assertEquals(0, context.scriptFields().fields().size());
        }
    }

    /**
     * test that creating more than the allowed number of scroll contexts throws an exception
     */
    public void testMaxOpenScrollContexts() throws Exception {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        // Open all possible scrolls, clear some of them, then open more until the limit is reached
        LinkedList<String> clearScrollIds = new LinkedList<>();

        for (int i = 0; i < SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY); i++) {
            assertResponse(client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)), searchResponse -> {
                if (randomInt(4) == 0) clearScrollIds.addLast(searchResponse.getScrollId());
            });
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.setScrollIds(clearScrollIds);
        client().clearScroll(clearScrollRequest).get();

        for (int i = 0; i < clearScrollIds.size(); i++) {
            client().prepareSearch("index").setSize(1).setScroll(TimeValue.timeValueMinutes(1)).get().decRef();
        }

        final ShardScrollRequestTest request = new ShardScrollRequestTest(indexShard.shardId());
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> service.createAndPutReaderContext(
                request,
                indexService,
                indexShard,
                indexShard.acquireSearcherSupplier(),
                SearchService.KEEPALIVE_INTERVAL_SETTING.get(Settings.EMPTY).millis()
            )
        );
        assertEquals(
            "Trying to create too many scroll contexts. Must be less than or equal to: ["
                + SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY)
                + "]. "
                + "This limit can be set by changing the [search.max_open_scroll_context] setting.",
            ex.getMessage()
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ex.status());

        service.freeAllScrollContexts();
    }

    public void testOpenScrollContextsConcurrently() throws Exception {
        createIndex("index");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        final int maxScrollContexts = SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY);
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        CountDownLatch latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (;;) {
                        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
                        try {
                            final ShardScrollRequestTest request = new ShardScrollRequestTest(indexShard.shardId());
                            searchService.createAndPutReaderContext(
                                request,
                                indexService,
                                indexShard,
                                reader,
                                SearchService.KEEPALIVE_INTERVAL_SETTING.get(Settings.EMPTY).millis()
                            );
                        } catch (ElasticsearchException e) {
                            assertThat(
                                e.getMessage(),
                                equalTo(
                                    "Trying to create too many scroll contexts. Must be less than or equal to: "
                                        + "["
                                        + maxScrollContexts
                                        + "]. "
                                        + "This limit can be set by changing the [search.max_open_scroll_context] setting."
                                )
                            );
                            return;
                        }
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("elasticsearch[node_s_0][search]");
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(searchService.getActiveContexts(), equalTo(maxScrollContexts));
        searchService.freeAllScrollContexts();
    }

    public static class FailOnRewriteQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(new QuerySpec<>("fail_on_rewrite_query", FailOnRewriteQueryBuilder::new, parseContext -> {
                throw new UnsupportedOperationException("No query parser for this plugin");
            }));
        }
    }

    public static class FailOnRewriteQueryBuilder extends DummyQueryBuilder {

        public FailOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        public FailOnRewriteQueryBuilder() {}

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            if (queryRewriteContext.convertToSearchExecutionContext() != null) {
                throw new IllegalStateException("Fail on rewrite phase");
            }
            return this;
        }
    }

    private static class ShardScrollRequestTest extends ShardSearchRequest {
        private final TimeValue scroll;

        ShardScrollRequestTest(ShardId shardId) {
            super(
                OriginalIndices.NONE,
                new SearchRequest().allowPartialSearchResults(true),
                shardId,
                0,
                1,
                AliasFilter.EMPTY,
                1f,
                -1,
                null
            );
            this.scroll = TimeValue.timeValueMinutes(1);
        }

        @Override
        public TimeValue scroll() {
            return this.scroll;
        }
    }

    public void testCanMatch() throws Exception {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder());
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );

        searchRequest.source(
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0))
        );
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );
        searchRequest.source(
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder()).aggregation(new GlobalAggregationBuilder("test"))
        );
        assertTrue(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
            ).canMatch()
        );
        assertEquals(5, numWrapInvocations.get());

        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );

        /*
         * Checks that canMatch takes into account the alias filter
         */
        // the source cannot be rewritten to a match_none
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    0,
                    1,
                    AliasFilter.of(new TermQueryBuilder("foo", "bar"), "alias"),
                    1f,
                    -1,
                    null
                )
            ).canMatch()
        );
        // the source can match and can be rewritten to a match_none, but not the alias filter
        final DocWriteResponse response = prepareIndex("index").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new TermQueryBuilder("id", "1")));
        assertFalse(
            service.canMatch(
                new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    0,
                    1,
                    AliasFilter.of(new TermQueryBuilder("foo", "bar"), "alias"),
                    1f,
                    -1,
                    null
                )
            ).canMatch()
        );

        CountDownLatch latch = new CountDownLatch(1);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());
        // Because the foo field used in alias filter is unmapped the term query builder rewrite can resolve to a match no docs query,
        // without acquiring a searcher and that means the wrapper is not called
        assertEquals(5, numWrapInvocations.get());
        service.executeQueryPhase(request, task, new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    // make sure that the wrapper is called when the query is actually executed
                    assertEquals(6, numWrapInvocations.get());
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    throw new AssertionError(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        latch.await();
    }

    public void testCanRewriteToMatchNone() {
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder()).aggregation(new GlobalAggregationBuilder("test"))
            )
        );
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder()));
        assertFalse(SearchService.canRewriteToMatchNone(null));
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0))
            )
        );
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))));
        assertTrue(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))
            )
        );
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                    .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))
                    .suggest(new SuggestBuilder())
            )
        );
        assertFalse(
            SearchService.canRewriteToMatchNone(
                new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar")).suggest(new SuggestBuilder())
            )
        );
    }

    public void testAggContextGetsMatchAll() throws IOException {
        createIndex("test");
        withAggregationContext("test", context -> assertThat(context.query(), equalTo(new MatchAllDocsQuery())));
    }

    public void testAggContextGetsNestedFilter() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject().startObject("properties");
        mapping.startObject("nested").field("type", "nested").endObject();
        mapping.endObject().endObject();

        createIndex("test", Settings.EMPTY, mapping);
        withAggregationContext("test", context -> assertThat(context.query(), equalTo(new MatchAllDocsQuery())));
    }

    /**
     * Build an {@link AggregationContext} with the named index.
     */
    private void withAggregationContext(String index, Consumer<AggregationContext> check) throws IOException {
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index));
        ShardId shardId = new ShardId(indexService.index(), 0);

        SearchRequest request = new SearchRequest().indices(index)
            .source(new SearchSourceBuilder().aggregation(new FiltersAggregationBuilder("test", new MatchAllQueryBuilder())))
            .allowPartialSearchResults(false);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            request,
            shardId,
            0,
            1,
            AliasFilter.EMPTY,
            1,
            0,
            null
        );

        try (ReaderContext readerContext = createReaderContext(indexService, indexService.getShard(0))) {
            try (
                SearchContext context = getInstanceFromNode(SearchService.class).createContext(
                    readerContext,
                    shardRequest,
                    mock(SearchShardTask.class),
                    ResultsType.QUERY,
                    true
                )
            ) {
                check.accept(context.aggregations().factories().context());
            }
        }
    }

    public void testExpandSearchFrozen() {
        String indexName = "frozen_index";
        createIndex(indexName);
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request(indexName, "index.frozen", "true")
        ).actionGet();

        prepareIndex(indexName).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch(), 0L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED), 1L);
        assertWarnings(TransportSearchAction.FROZEN_INDICES_DEPRECATION_MESSAGE.replace("{}", indexName));
    }

    public void testCreateReduceContext() {
        SearchService service = getInstanceFromNode(SearchService.class);
        AggregationReduceContext.Builder reduceContextBuilder = service.aggReduceContextBuilder(
            () -> false,
            new SearchRequest().source(new SearchSourceBuilder()).source().aggregations()
        );
        {
            AggregationReduceContext reduceContext = reduceContextBuilder.forFinalReduction();
            expectThrows(
                MultiBucketConsumerService.TooManyBucketsException.class,
                () -> reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1)
            );
        }
        {
            AggregationReduceContext reduceContext = reduceContextBuilder.forPartialReduction();
            reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1);
        }
    }

    public void testMultiBucketConsumerServiceCB() {
        MultiBucketConsumerService service = new MultiBucketConsumerService(
            getInstanceFromNode(ClusterService.class),
            Settings.EMPTY,
            new NoopCircuitBreaker("test") {

                @Override
                public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                    throw new CircuitBreakingException("tripped", getDurability());
                }
            }
        );
        // for partial
        {
            IntConsumer consumer = service.createForPartial();
            for (int i = 0; i < 1023; i++) {
                consumer.accept(0);
            }
            CircuitBreakingException ex = expectThrows(CircuitBreakingException.class, () -> consumer.accept(0));
            assertThat(ex.getMessage(), equalTo("tripped"));
        }
        // for final
        {
            IntConsumer consumer = service.createForFinal();
            for (int i = 0; i < 1023; i++) {
                consumer.accept(0);
            }
            CircuitBreakingException ex = expectThrows(CircuitBreakingException.class, () -> consumer.accept(0));
            assertThat(ex.getMessage(), equalTo("tripped"));
        }
    }

    public void testCreateSearchContext() throws IOException {
        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            0,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            nowInMillis,
            clusterAlias
        );
        try (SearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
            SearchShardTarget searchShardTarget = searchContext.shardTarget();
            SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
            String expectedIndexName = clusterAlias == null ? index : clusterAlias + ":" + index;
            assertEquals(expectedIndexName, searchExecutionContext.getFullyQualifiedIndex().getName());
            assertEquals(expectedIndexName, searchShardTarget.getFullyQualifiedIndexName());
            assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
            assertEquals(shardId, searchShardTarget.getShardId());

            assertNull(searchContext.dfsResult());
            searchContext.addDfsResult();
            assertSame(searchShardTarget, searchContext.dfsResult().getSearchShardTarget());

            assertNull(searchContext.queryResult());
            searchContext.addQueryResult();
            assertSame(searchShardTarget, searchContext.queryResult().getSearchShardTarget());

            assertNull(searchContext.fetchResult());
            searchContext.addFetchResult();
            assertSame(searchShardTarget, searchContext.fetchResult().getSearchShardTarget());
        }
    }

    /**
     * While we have no NPE in DefaultContext constructor anymore, we still want to guard against it (or other failures) in the future to
     * avoid leaking searchers.
     */
    public void testCreateSearchContextFailure() throws Exception {
        final String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        final IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        final ShardId shardId = new ShardId(indexService.index(), 0);
        final ShardSearchRequest request = new ShardSearchRequest(shardId, 0, null) {
            @Override
            public SearchType searchType() {
                // induce an artificial NPE
                throw new NullPointerException("expected");
            }
        };
        try (ReaderContext reader = createReaderContext(indexService, indexService.getShard(shardId.id()))) {
            NullPointerException e = expectThrows(
                NullPointerException.class,
                () -> service.createContext(reader, request, mock(SearchShardTask.class), ResultsType.NONE, randomBoolean())
            );
            assertEquals("expected", e.getMessage());
        }
        // Needs to busily assert because Engine#refreshNeeded can increase the refCount.
        assertBusy(
            () -> assertEquals("should have 2 store refs (IndexService + InternalEngine)", 2, indexService.getShard(0).store().refCount())
        );
    }

    public void testMatchNoDocsEmptyResponse() throws InterruptedException {
        createIndex("index");
        Thread currentThread = Thread.currentThread();
        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().aggregation(AggregationBuilders.count("count").field("value")));
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            5,
            AliasFilter.EMPTY,
            1.0f,
            0,
            null
        );
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchAllQueryBuilder());
            service.executeQueryPhase(shardRequest, task, new ActionListener<>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchNoneQueryBuilder());
            service.executeQueryPhase(shardRequest, task, new ActionListener<>() {

                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.canReturnNullResponseIfMatchNoDocs(true);
            service.executeQueryPhase(shardRequest, task, new ActionListener<>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        // make sure we don't use the search threadpool
                        assertSame(Thread.currentThread(), currentThread);
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertTrue(result.queryResult().isNull());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        throw new AssertionError(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }
    }

    public void testDeleteIndexWhileSearch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(1, 20);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setSource("f", "v").get();
        }
        indicesAdmin().prepareRefresh("test").get();
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchers = new Thread[randomIntBetween(1, 4)];
        CountDownLatch latch = new CountDownLatch(searchers.length);
        for (int i = 0; i < searchers.length; i++) {
            searchers[i] = new Thread(() -> {
                latch.countDown();
                while (stopped.get() == false) {
                    try {
                        client().prepareSearch("test").setRequestCache(false).get().decRef();
                    } catch (Exception ignored) {
                        return;
                    }
                }
            });
            searchers[i].start();
        }
        latch.await();
        indicesAdmin().prepareDelete("test").get();
        stopped.set(true);
        for (Thread searcher : searchers) {
            searcher.join();
        }
    }

    public void testLookUpSearchContext() throws Exception {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        List<ShardSearchContextId> contextIds = new ArrayList<>();
        int numContexts = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(1);
        indexShard.getThreadPool().executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                for (int i = 0; i < numContexts; i++) {
                    ShardSearchRequest request = new ShardSearchRequest(
                        OriginalIndices.NONE,
                        new SearchRequest().allowPartialSearchResults(true),
                        indexShard.shardId(),
                        0,
                        1,
                        AliasFilter.EMPTY,
                        1.0f,
                        -1,
                        null
                    );
                    final ReaderContext context = searchService.createAndPutReaderContext(
                        request,
                        indexService,
                        indexShard,
                        indexShard.acquireSearcherSupplier(),
                        SearchService.KEEPALIVE_INTERVAL_SETTING.get(Settings.EMPTY).millis()
                    );
                    assertThat(context.id().getId(), equalTo((long) (i + 1)));
                    contextIds.add(context.id());
                }
                assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                while (contextIds.isEmpty() == false) {
                    final ShardSearchContextId contextId = randomFrom(contextIds);
                    assertFalse(searchService.freeReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), contextId.getId())));
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    if (randomBoolean()) {
                        assertTrue(searchService.freeReaderContext(contextId));
                    } else {
                        assertTrue(
                            searchService.freeReaderContext((new ShardSearchContextId(contextId.getSessionId(), contextId.getId())))
                        );
                    }
                    contextIds.remove(contextId);
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    assertFalse(searchService.freeReaderContext(contextId));
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                }
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    public void testOpenReaderContext() {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.openReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        future.actionGet();
        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertTrue(searchService.freeReaderContext(future.actionGet()));
    }

    public void testCancelQueryPhaseEarly() throws Exception {
        createIndex("index");
        final MockSearchService service = (MockSearchService) getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );

        CountDownLatch latch1 = new CountDownLatch(1);
        SearchShardTask task = new SearchShardTask(1, "", "", "", TaskId.EMPTY_TASK_ID, emptyMap());
        service.executeQueryPhase(request, task, new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                service.freeReaderContext(searchPhaseResult.getContextId());
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    fail("Search should not be cancelled");
                } finally {
                    latch1.countDown();
                }
            }
        });
        latch1.await();

        CountDownLatch latch2 = new CountDownLatch(1);
        service.executeDfsPhase(request, task, new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                service.freeReaderContext(searchPhaseResult.getContextId());
                latch2.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    fail("Search should not be cancelled");
                } finally {
                    latch2.countDown();
                }
            }
        });
        latch2.await();

        AtomicBoolean searchContextCreated = new AtomicBoolean(false);
        service.setOnCreateSearchContext(c -> searchContextCreated.set(true));
        CountDownLatch latch3 = new CountDownLatch(1);
        TaskCancelHelper.cancel(task, "simulated");
        service.executeQueryPhase(request, task, new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    fail("Search not cancelled early");
                } finally {
                    service.freeReaderContext(searchPhaseResult.getContextId());
                    searchPhaseResult.decRef();
                    latch3.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, is(instanceOf(TaskCancelledException.class)));
                assertThat(e.getMessage(), is("task cancelled [simulated]"));
                assertThat(((TaskCancelledException) e).status(), is(RestStatus.BAD_REQUEST));
                assertThat(searchContextCreated.get(), is(false));
                latch3.countDown();
            }
        });
        latch3.await();

        searchContextCreated.set(false);
        CountDownLatch latch4 = new CountDownLatch(1);
        service.executeDfsPhase(request, task, new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    fail("Search not cancelled early");
                } finally {
                    service.freeReaderContext(searchPhaseResult.getContextId());
                    latch4.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, is(instanceOf(TaskCancelledException.class)));
                assertThat(e.getMessage(), is("task cancelled [simulated]"));
                assertThat(((TaskCancelledException) e).status(), is(RestStatus.BAD_REQUEST));
                assertThat(searchContextCreated.get(), is(false));
                latch4.countDown();
            }
        });
        latch4.await();
    }

    public void testCancelFetchPhaseEarly() throws Exception {
        createIndex("index");
        final MockSearchService service = (MockSearchService) getInstanceFromNode(SearchService.class);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);

        AtomicBoolean searchContextCreated = new AtomicBoolean(false);
        service.setOnCreateSearchContext(c -> searchContextCreated.set(true));

        // Test fetch phase is cancelled early
        String scrollId;
        var searchResponse = client().search(searchRequest.allowPartialSearchResults(false).scroll(TimeValue.timeValueMinutes(10))).get();
        try {
            scrollId = searchResponse.getScrollId();
        } finally {
            searchResponse.decRef();
        }

        client().searchScroll(new SearchScrollRequest(scrollId)).get().decRef();
        assertThat(searchContextCreated.get(), is(true));

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client().clearScroll(clearScrollRequest);

        searchResponse = client().search(searchRequest.allowPartialSearchResults(false).scroll(TimeValue.timeValueMinutes(10))).get();
        try {
            scrollId = searchResponse.getScrollId();
        } finally {
            searchResponse.decRef();
        }
        searchContextCreated.set(false);
        service.setOnCheckCancelled(t -> {
            SearchShardTask task = new SearchShardTask(randomLong(), "transport", "action", "", TaskId.EMPTY_TASK_ID, emptyMap());
            TaskCancelHelper.cancel(task, "simulated");
            return task;
        });
        CountDownLatch latch = new CountDownLatch(1);
        client().searchScroll(new SearchScrollRequest(scrollId), new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    fail("Search not cancelled early");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                Throwable cancelledExc = e.getCause().getCause();
                assertThat(cancelledExc, is(instanceOf(TaskCancelledException.class)));
                assertThat(cancelledExc.getMessage(), is("task cancelled [simulated]"));
                assertThat(((TaskCancelledException) cancelledExc).status(), is(RestStatus.BAD_REQUEST));
                latch.countDown();
            }
        });
        latch.await();
        assertThat(searchContextCreated.get(), is(false));

        clearScrollRequest.setScrollIds(singletonList(scrollId));
        client().clearScroll(clearScrollRequest);
    }

    public void testWaitOnRefresh() throws ExecutionException, InterruptedException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setWaitForCheckpointsTimeout(TimeValue.timeValueSeconds(30));
        searchRequest.setWaitForCheckpoints(Collections.singletonMap("index", new long[] { 0 }));

        final DocWriteResponse response = prepareIndex("index").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());

        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null,
            null,
            null
        );
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        service.executeQueryPhase(request, task, future.delegateFailure((l, r) -> {
            assertEquals(1, r.queryResult().getTotalHits().value());
            l.onResponse(null);
        }));
        future.get();
    }

    public void testWaitOnRefreshFailsWithRefreshesDisabled() {
        createIndex("index", Settings.builder().put("index.refresh_interval", "-1").build());
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setWaitForCheckpointsTimeout(TimeValue.timeValueSeconds(30));
        searchRequest.setWaitForCheckpoints(Collections.singletonMap("index", new long[] { 0 }));

        final DocWriteResponse response = prepareIndex("index").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());

        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());
        PlainActionFuture<SearchPhaseResult> future = new PlainActionFuture<>();
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null,
            null,
            null
        );
        service.executeQueryPhase(request, task, future);
        IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            illegalArgumentException.getMessage(),
            containsString("Cannot use wait_for_checkpoints with [index.refresh_interval=-1]")
        );
    }

    public void testWaitOnRefreshFailsIfCheckpointNotIndexed() {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        // Increased timeout to avoid cancelling the search task prior to its completion,
        // as we expect to raise an Exception. Timeout itself is tested on the following `testWaitOnRefreshTimeout` test.
        searchRequest.setWaitForCheckpointsTimeout(TimeValue.timeValueMillis(randomIntBetween(200, 300)));
        searchRequest.setWaitForCheckpoints(Collections.singletonMap("index", new long[] { 1 }));

        final DocWriteResponse response = prepareIndex("index").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());

        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());
        PlainActionFuture<SearchPhaseResult> future = new PlainActionFuture<>();
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null,
            null,
            null
        );
        service.executeQueryPhase(request, task, future);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            ex.getMessage(),
            containsString("Cannot wait for unissued seqNo checkpoint [wait_for_checkpoint=1, max_issued_seqNo=0]")
        );
    }

    public void testWaitOnRefreshTimeout() {
        createIndex("index", Settings.builder().put("index.refresh_interval", "60s").build());
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setWaitForCheckpointsTimeout(TimeValue.timeValueMillis(randomIntBetween(10, 100)));
        searchRequest.setWaitForCheckpoints(Collections.singletonMap("index", new long[] { 0 }));

        final DocWriteResponse response = prepareIndex("index").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());

        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, emptyMap());
        PlainActionFuture<SearchPhaseResult> future = new PlainActionFuture<>();
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null,
            null,
            null
        );
        service.executeQueryPhase(request, task, future);

        SearchTimeoutException ex = expectThrows(SearchTimeoutException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("Wait for seq_no [0] refreshed timed out ["));
    }

    public void testMinimalSearchSourceInShardRequests() {
        createIndex("test");
        int numDocs = between(0, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setSource("id", Integer.toString(i)).get();
        }
        indicesAdmin().prepareRefresh("test").get();

        BytesReference pitId = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueMinutes(10))
        ).actionGet().getPointInTimeId();
        final MockSearchService searchService = (MockSearchService) getInstanceFromNode(SearchService.class);
        final List<ShardSearchRequest> shardRequests = new CopyOnWriteArrayList<>();
        searchService.setOnCreateSearchContext(ctx -> shardRequests.add(ctx.request()));
        try {
            assertHitCount(
                client().prepareSearch()
                    .setSource(
                        new SearchSourceBuilder().size(between(numDocs, numDocs * 2)).pointInTimeBuilder(new PointInTimeBuilder(pitId))
                    ),
                numDocs
            );
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
        assertThat(shardRequests, not(emptyList()));
        for (ShardSearchRequest shardRequest : shardRequests) {
            assertNotNull(shardRequest.source());
            assertNotNull(shardRequest.source().pointInTimeBuilder());
            assertThat(shardRequest.source().pointInTimeBuilder().getEncodedId(), equalTo(BytesArray.EMPTY));
        }
    }

    public void testDfsQueryPhaseRewrite() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.source(SearchSourceBuilder.searchSource().query(new TestRewriteCounterQueryBuilder()));
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
        ReaderContext context = service.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            reader,
            SearchService.KEEPALIVE_INTERVAL_SETTING.get(Settings.EMPTY).millis()
        );
        PlainActionFuture<QuerySearchResult> plainActionFuture = new PlainActionFuture<>();
        service.executeQueryPhase(
            new QuerySearchRequest(null, context.id(), request, new AggregatedDfs(Map.of(), Map.of(), 10)),
            new SearchShardTask(42L, "", "", "", null, emptyMap()),
            plainActionFuture,
            TransportVersion.current()
        );

        plainActionFuture.actionGet();
        assertThat(((TestRewriteCounterQueryBuilder) request.source().query()).asyncRewriteCount, equalTo(1));
        final ShardSearchContextId contextId = context.id();
        assertTrue(service.freeReaderContext(contextId));
    }

    public void testEnableSearchWorkerThreads() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY);
        IndexShard indexShard = indexService.getShard(0);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(randomBoolean()),
            indexShard.shardId(),
            0,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null
        );
        try (ReaderContext readerContext = createReaderContext(indexService, indexShard)) {
            SearchService service = getInstanceFromNode(SearchService.class);
            SearchShardTask task = new SearchShardTask(0, "type", "action", "description", null, emptyMap());

            try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.DFS, randomBoolean())) {
                assertTrue(searchContext.searcher().hasExecutor());
            }

            try {
                ClusterUpdateSettingsResponse response = client().admin()
                    .cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().put(SEARCH_WORKER_THREADS_ENABLED.getKey(), false).build())
                    .get();
                assertTrue(response.isAcknowledged());
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.DFS, randomBoolean())) {
                    assertFalse(searchContext.searcher().hasExecutor());
                }
            } finally {
                // reset original default setting
                client().admin()
                    .cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(SEARCH_WORKER_THREADS_ENABLED.getKey()).build())
                    .get();
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.DFS, randomBoolean())) {
                    assertTrue(searchContext.searcher().hasExecutor());
                }
            }
        }
    }

    /**
     * Verify that a single slice is created for requests that don't support parallel collection, while an executor is still
     * provided to the searcher to parallelize other operations. Also ensure multiple slices are created for requests that do support
     * parallel collection.
     */
    public void testSlicingBehaviourForParallelCollection() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) indexService.getThreadPool().executor(ThreadPool.Names.SEARCH);

        // We configure the executor pool size explicitly in nodeSettings to be independent of CPU cores
        assert String.valueOf(SEARCH_POOL_SIZE).equals(node().settings().get("thread_pool.search.size"))
            : "Unexpected thread_pool.search.size";

        int numDocs = randomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("index").setId(String.valueOf(i)).setSource("field", "value").get();
            if (i % 5 == 0) {
                indicesAdmin().prepareRefresh("index").get();
            }
        }
        final IndexShard indexShard = indexService.getShard(0);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(randomBoolean()),
            indexShard.shardId(),
            0,
            indexService.numberOfShards(),
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null
        );
        SearchService service = getInstanceFromNode(SearchService.class);
        NonCountingTermQuery termQuery = new NonCountingTermQuery(new Term("field", "value"));
        assertEquals(0, executor.getCompletedTaskCount());
        try (ReaderContext readerContext = createReaderContext(indexService, indexShard)) {
            SearchShardTask task = new SearchShardTask(0, "type", "action", "description", null, emptyMap());
            {
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.DFS, true)) {
                    ContextIndexSearcher searcher = searchContext.searcher();
                    assertTrue(searcher.hasExecutor());

                    final int maxPoolSize = executor.getMaximumPoolSize();
                    assertEquals(
                        "Sanity check to ensure this isn't the default of 1 when pool size is unset",
                        SEARCH_POOL_SIZE,
                        maxPoolSize
                    );

                    final int expectedSlices = ContextIndexSearcher.computeSlices(
                        searcher.getIndexReader().leaves(),
                        maxPoolSize,
                        1
                    ).length;
                    assertNotEquals("Sanity check to ensure this isn't the default of 1 when pool size is unset", 1, expectedSlices);

                    final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                    searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                    assertBusy(
                        () -> assertEquals(
                            "DFS supports parallel collection, so the number of slices should be > 1.",
                            expectedSlices - 1, // one slice executes on the calling thread
                            executor.getCompletedTaskCount() - priorExecutorTaskCount
                        )
                    );
                }
            }
            {
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.QUERY, true)) {
                    ContextIndexSearcher searcher = searchContext.searcher();
                    assertTrue(searcher.hasExecutor());

                    final int maxPoolSize = executor.getMaximumPoolSize();
                    assertEquals(
                        "Sanity check to ensure this isn't the default of 1 when pool size is unset",
                        SEARCH_POOL_SIZE,
                        maxPoolSize
                    );

                    final int expectedSlices = ContextIndexSearcher.computeSlices(
                        searcher.getIndexReader().leaves(),
                        maxPoolSize,
                        1
                    ).length;
                    assertNotEquals("Sanity check to ensure this isn't the default of 1 when pool size is unset", 1, expectedSlices);

                    final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                    searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                    assertBusy(
                        () -> assertEquals(
                            "QUERY supports parallel collection when enabled, so the number of slices should be > 1.",
                            expectedSlices - 1, // one slice executes on the calling thread
                            executor.getCompletedTaskCount() - priorExecutorTaskCount
                        )
                    );
                }
            }
            {
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.FETCH, true)) {
                    ContextIndexSearcher searcher = searchContext.searcher();
                    assertFalse(searcher.hasExecutor());
                    final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                    searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                    assertBusy(
                        () -> assertEquals(
                            "The number of slices should be 1 as FETCH does not support parallel collection and thus runs on the calling"
                                + " thread.",
                            0,
                            executor.getCompletedTaskCount() - priorExecutorTaskCount
                        )
                    );
                }
            }
            {
                try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.NONE, true)) {
                    ContextIndexSearcher searcher = searchContext.searcher();
                    assertFalse(searcher.hasExecutor());
                    final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                    searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                    assertBusy(
                        () -> assertEquals(
                            "The number of slices should be 1 as NONE does not support parallel collection.",
                            0, // zero since one slice executes on the calling thread
                            executor.getCompletedTaskCount() - priorExecutorTaskCount
                        )
                    );
                }
            }

            try {
                ClusterUpdateSettingsResponse response = client().admin()
                    .cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().put(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), false).build())
                    .get();
                assertTrue(response.isAcknowledged());
                {
                    try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.QUERY, true)) {
                        ContextIndexSearcher searcher = searchContext.searcher();
                        assertFalse(searcher.hasExecutor());
                        final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                        searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                        assertBusy(
                            () -> assertEquals(
                                "The number of slices should be 1 when QUERY parallel collection is disabled.",
                                0, // zero since one slice executes on the calling thread
                                executor.getCompletedTaskCount() - priorExecutorTaskCount
                            )
                        );
                    }
                }
            } finally {
                // Reset to the original default setting and check to ensure it takes effect.
                client().admin()
                    .cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey()).build())
                    .get();
                {
                    try (SearchContext searchContext = service.createContext(readerContext, request, task, ResultsType.QUERY, true)) {
                        ContextIndexSearcher searcher = searchContext.searcher();
                        assertTrue(searcher.hasExecutor());

                        final int maxPoolSize = executor.getMaximumPoolSize();
                        assertEquals(
                            "Sanity check to ensure this isn't the default of 1 when pool size is unset",
                            SEARCH_POOL_SIZE,
                            maxPoolSize
                        );

                        final int expectedSlices = ContextIndexSearcher.computeSlices(
                            searcher.getIndexReader().leaves(),
                            maxPoolSize,
                            1
                        ).length;
                        assertNotEquals("Sanity check to ensure this isn't the default of 1 when pool size is unset", 1, expectedSlices);

                        final long priorExecutorTaskCount = executor.getCompletedTaskCount();
                        searcher.search(termQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                        assertBusy(
                            () -> assertEquals(
                                "QUERY supports parallel collection when enabled, so the number of slices should be > 1.",
                                expectedSlices - 1, // one slice executes on the calling thread
                                executor.getCompletedTaskCount() - priorExecutorTaskCount
                            )
                        );
                    }
                }
            }
        }
    }

    private static ReaderContext createReaderContext(IndexService indexService, IndexShard indexShard) {
        return new ReaderContext(
            new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            randomNonNegativeLong(),
            false
        );
    }

    private static class TestRewriteCounterQueryBuilder extends AbstractQueryBuilder<TestRewriteCounterQueryBuilder> {

        final int asyncRewriteCount;
        final Supplier<Boolean> fetched;

        TestRewriteCounterQueryBuilder() {
            asyncRewriteCount = 0;
            fetched = null;
        }

        private TestRewriteCounterQueryBuilder(int asyncRewriteCount, Supplier<Boolean> fetched) {
            this.asyncRewriteCount = asyncRewriteCount;
            this.fetched = fetched;
        }

        @Override
        public String getWriteableName() {
            return "test_query";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {}

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            return new MatchAllDocsQuery();
        }

        @Override
        protected boolean doEquals(TestRewriteCounterQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 42;
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            if (asyncRewriteCount > 0) {
                return this;
            }
            if (fetched != null) {
                if (fetched.get() == null) {
                    return this;
                }
                assert fetched.get();
                return new TestRewriteCounterQueryBuilder(1, null);
            }
            if (queryRewriteContext.convertToDataRewriteContext() != null) {
                SetOnce<Boolean> awaitingFetch = new SetOnce<>();
                queryRewriteContext.registerAsyncAction((c, l) -> {
                    awaitingFetch.set(true);
                    l.onResponse(null);
                });
                return new TestRewriteCounterQueryBuilder(0, awaitingFetch::get);
            }
            return this;
        }
    }
}
