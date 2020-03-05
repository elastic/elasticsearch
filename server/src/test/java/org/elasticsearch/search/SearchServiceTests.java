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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class SearchServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FailOnRewriteQueryPlugin.class, CustomScriptPlugin.class,
            ReaderWrapperCountPlugin.class, InternalOrPrivateSettingsPlugin.class);
    }

    public static class ReaderWrapperCountPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(service -> SearchServiceTests::apply);
        }
    }

    @Before
    private void resetCount() {
        numWrapInvocations = new AtomicInteger(0);
    }

    private static AtomicInteger numWrapInvocations = new AtomicInteger(0);
    private static DirectoryReader apply(DirectoryReader directoryReader) throws IOException {
        numWrapInvocations.incrementAndGet();
        return new FilterDirectoryReader(directoryReader,
            new FilterDirectoryReader.SubReaderWrapper() {
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
                public void onNewContext(SearchContext context) {
                    if (context.query() != null) {
                        if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                            assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search_throttled]"));
                        } else {
                            assertThat(Thread.currentThread().getName(), startsWith("elasticsearch[node_s_0][search]"));
                        }
                    }
                }

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
        return Settings.builder().put("search.default_search_timeout", "5s").build();
    }

    public void testClearOnClose() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }

    public void testCloseSearchContextOnRewriteException() {
        // if refresh happens while checking the exception, the subsequent reference count might not match, so we switch it off
        createIndex("index", Settings.builder().put("index.refresh_interval", -1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        final int activeContexts = service.getActiveContexts();
        final int activeRefs = indexShard.store().refCount();
        expectThrows(SearchPhaseExecutionException.class, () ->
                client().prepareSearch("index").setQuery(new FailOnRewriteQueryBuilder()).get());
        assertEquals(activeContexts, service.getActiveContexts());
        assertEquals(activeRefs, indexShard.store().refCount());
    }

    public void testSearchWhileIndexDeleted() throws InterruptedException {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startGun = new CountDownLatch(1);
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

        final Thread thread = new Thread() {
            @Override
            public void run() {
                startGun.countDown();
                while(running.get()) {
                    service.afterIndexRemoved(indexService.index(), indexService.getIndexSettings(), DELETED);
                    if (randomBoolean()) {
                        // here we trigger some refreshes to ensure the IR go out of scope such that we hit ACE if we access a search
                        // context in a non-sane way.
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        client().prepareIndex("index").setSource("field", "value")
                            .setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values())).execute(new ActionListener<IndexResponse>() {
                            @Override
                            public void onResponse(IndexResponse indexResponse) {
                                semaphore.release();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                semaphore.release();
                            }
                        });
                    }
                }
            }
        };
        thread.start();
        startGun.await();
        try {
            final int rounds = scaledRandomIntBetween(100, 10000);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true)
                .scroll(new Scroll(TimeValue.timeValueMinutes(1)));
            for (int i = 0; i < rounds; i++) {
                try {
                    try {
                        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
                        final boolean useScroll = randomBoolean();
                        service.executeQueryPhase(
                            new ShardSearchRequest(OriginalIndices.NONE, useScroll ? scrollSearchRequest : searchRequest,
                                indexShard.shardId(), 1,
                                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
                            new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), result);
                        SearchPhaseResult searchPhaseResult = result.get();
                        IntArrayList intCursors = new IntArrayList(1);
                        intCursors.add(0);
                        ShardFetchRequest req = new ShardFetchRequest(searchPhaseResult.getRequestId(), intCursors, null/* not a scroll */);
                        PlainActionFuture<FetchSearchResult> listener = new PlainActionFuture<>();
                        service.executeFetchPhase(req, new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), listener);
                        listener.get();
                        if (useScroll) {
                            service.freeContext(searchPhaseResult.getRequestId());
                        }
                    } catch (ExecutionException ex) {
                        assertThat(ex.getCause(), instanceOf(RuntimeException.class));
                        throw ((RuntimeException)ex.getCause());
                    }
                } catch (AlreadyClosedException ex) {
                    throw ex;
                } catch (IllegalStateException ex) {
                    assertEquals("search context is already closed can't increment refCount current count [0]", ex.getMessage());
                } catch (SearchContextMissingException ex) {
                    // that's fine
                }
            }
        } finally {
            running.set(false);
            thread.join();
            semaphore.acquire(Integer.MAX_VALUE);
        }

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testTimeout() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(
            new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
            indexShard);
        final SearchContext contextWithDefaultTimeout = service.createContext(rewriteContext);
        try {
            // the search context should inherit the default timeout
            assertThat(contextWithDefaultTimeout.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        } finally {
            contextWithDefaultTimeout.decRef();
            service.freeContext(contextWithDefaultTimeout.id());
        }

        final long seconds = randomIntBetween(6, 10);
        searchRequest.source(new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(seconds)));
        rewriteContext = service.acquireSearcherAndRewrite(
            new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
            indexShard);
        final SearchContext context = service.createContext(rewriteContext);
        try {
            // the search context should inherit the query timeout
            assertThat(context.timeout(), equalTo(TimeValue.timeValueSeconds(seconds)));
        } finally {
            context.decRef();
            service.freeContext(context.id());
        }

    }

    /**
     * test that getting more than the allowed number of docvalue_fields throws an exception
     */
    public void testMaxDocvalueFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of docvalue_fields to retrieve
        for (int i = 0; i < indexService.getIndexSettings().getMaxDocvalueFields(); i++) {
            searchSourceBuilder.docValueField("field" + i);
        }

        ShardSearchRequest shardRequest =  new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);

        {
            SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(shardRequest, indexShard);
            try (SearchContext context = service.createContext(rewriteContext)) {
                assertNotNull(context);
            }
        }

        {
            SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(shardRequest, indexShard);
            searchSourceBuilder.docValueField("one_field_too_much");
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> service.createContext(rewriteContext));
            assertEquals(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [100] but was [101]. "
                    + "This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.", ex.getMessage());
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
            searchSourceBuilder.scriptField("field" + i,
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
        }

        ShardSearchRequest shardRequest = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);

        {
            SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(shardRequest, indexShard);
            try (SearchContext context = service.createContext(rewriteContext)) {
                assertNotNull(context);
            }
        }

        {
            searchSourceBuilder.scriptField("anotherScriptField",
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
            SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(shardRequest, indexShard);
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                    () -> service.createContext(rewriteContext));
            assertEquals(
                "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxScriptFields + "] but was ["
                    + (maxScriptFields + 1)
                    + "]. This limit can be set by changing the [index.max_script_fields] index level setting.",
                ex.getMessage());
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
        searchSourceBuilder.scriptField("field" + 0,
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
        searchSourceBuilder.size(0);
        SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(
            new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
            indexShard);
        try (SearchContext context = service.createContext(rewriteContext)) {
            assertEquals(0, context.scriptFields().fields().size());
        }
    }

    /**
     * test that creating more than the allowed number of scroll contexts throws an exception
     */
    public void testMaxOpenScrollContexts() throws RuntimeException, IOException {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        // Open all possible scrolls, clear some of them, then open more until the limit is reached
        LinkedList<String> clearScrollIds = new LinkedList<>();

        for (int i = 0; i < SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY); i++) {
            SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();

            if (randomInt(4) == 0) clearScrollIds.addLast(searchResponse.getScrollId());
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.setScrollIds(clearScrollIds);
        client().clearScroll(clearScrollRequest);

        for (int i = 0; i < clearScrollIds.size(); i++) {
            client().prepareSearch("index").setSize(1).setScroll("1m").get();
        }

        SearchService.SearchRewriteContext rewriteContext =
            service.acquireSearcherAndRewrite(new ShardScrollRequestTest(indexShard.shardId()), indexShard);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> service.createAndPutContext(rewriteContext));
        assertEquals(
            "Trying to create too many scroll contexts. Must be less than or equal to: [" +
                SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY) + "]. " +
                "This limit can be set by changing the [search.max_open_scroll_context] setting.",
            ex.getMessage());
    }

    public static class FailOnRewriteQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(new QuerySpec<>("fail_on_rewrite_query", FailOnRewriteQueryBuilder::new, parseContext -> {
                throw new UnsupportedOperationException("No query parser for this plugin");
            }));
        }
    }

    public static class FailOnRewriteQueryBuilder extends AbstractQueryBuilder<FailOnRewriteQueryBuilder> {

        public FailOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        public FailOnRewriteQueryBuilder() {
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            if (queryRewriteContext.convertToShardContext() != null) {
                throw new IllegalStateException("Fail on rewrite phase");
            }
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {
        }

        @Override
        protected Query doToQuery(QueryShardContext context) {
            return null;
        }

        @Override
        protected boolean doEquals(FailOnRewriteQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return null;
        }
    }

    private static class ShardScrollRequestTest extends ShardSearchRequest {
        private Scroll scroll;

        ShardScrollRequestTest(ShardId shardId) {
            super(OriginalIndices.NONE, new SearchRequest().allowPartialSearchResults(true),
                shardId, 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null);
            this.scroll = new Scroll(TimeValue.timeValueMinutes(1));
        }

        @Override
        public Scroll scroll() {
            return this.scroll;
        }
    }

    public void testCanMatch() throws IOException, InterruptedException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        int numWrapReader = numWrapInvocations.get();
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder());
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test", ValueType.STRING).minDocCount(0)));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1,  null, null)).canMatch());
        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new GlobalAggregationBuilder("test")));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        assertFalse(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());
        assertEquals(numWrapReader, numWrapInvocations.get());

        ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);

        CountDownLatch latch = new CountDownLatch(1);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
        service.executeQueryPhase(request, task, new ActionListener<SearchPhaseResult>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    // make sure that the wrapper is called when the query is actually executed
                    assertEquals(numWrapReader+1, numWrapInvocations.get());
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
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new GlobalAggregationBuilder("test"))));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder()));
        assertFalse(SearchService.canRewriteToMatchNone(null));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test", ValueType.STRING).minDocCount(0))));
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))));
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test", ValueType.STRING).minDocCount(1))));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test", ValueType.STRING).minDocCount(1))
            .suggest(new SuggestBuilder())));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))
            .suggest(new SuggestBuilder())));
    }

    public void testSetSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "true"))
            .actionGet();
        final SearchService service = getInstanceFromNode(SearchService.class);
        Index index = resolveIndex("throttled_threadpool_index");
        assertTrue(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        client().prepareIndex("throttled_threadpool_index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("throttled_threadpool_index")
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).setSize(1).get();
        assertSearchHits(searchResponse, "1");
        // we add a search action listener in a plugin above to assert that this is actually used
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "false"))
            .actionGet();

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
            client().admin().indices().prepareUpdateSettings("throttled_threadpool_index").setSettings(Settings.builder().put(IndexSettings
                .INDEX_SEARCH_THROTTLED.getKey(), false)).get());
        assertEquals("can not update private setting [index.search.throttled]; this setting is managed by Elasticsearch",
            iae.getMessage());
        assertFalse(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        ShardSearchRequest req = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, new ShardId(index, 0), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null);
        Thread currentThread = Thread.currentThread();
        // we still make sure can match is executed on the network thread
        service.canMatch(req, ActionListener.wrap(r -> assertSame(Thread.currentThread(), currentThread), e -> fail("unexpected")));
    }

    public void testExpandSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "true"))
            .actionGet();

        client().prepareIndex("throttled_threadpool_index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch().get(), 0L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1L);
    }

    public void testCreateReduceContext() {
        final SearchService service = getInstanceFromNode(SearchService.class);
        {
            InternalAggregation.ReduceContext reduceContext = service.createReduceContext(true);
            expectThrows(MultiBucketConsumerService.TooManyBucketsException.class,
                () -> reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1));
        }
        {
            InternalAggregation.ReduceContext reduceContext = service.createReduceContext(false);
            reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1);
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
        ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shardId,
            indexService.numberOfShards(), AliasFilter.EMPTY, 1f, nowInMillis, clusterAlias, Strings.EMPTY_ARRAY);
        try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
            SearchShardTarget searchShardTarget = searchContext.shardTarget();
            QueryShardContext queryShardContext = searchContext.getQueryShardContext();
            String expectedIndexName = clusterAlias == null ? index : clusterAlias + ":" + index;
            assertEquals(expectedIndexName, queryShardContext.getFullyQualifiedIndex().getName());
            assertEquals(expectedIndexName, searchShardTarget.getFullyQualifiedIndexName());
            assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
            assertEquals(shardId, searchShardTarget.getShardId());
            assertSame(searchShardTarget, searchContext.dfsResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.queryResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.fetchResult().getSearchShardTarget());
        }
    }

    /**
     * While we have no NPE in DefaultContext constructor anymore, we still want to guard against it (or other failures) in the future to
     * avoid leaking searchers.
     */
    public void testCreateSearchContextFailure() throws IOException {
        final String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        final IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        final ShardId shardId = new ShardId(indexService.index(), 0);
        IndexShard indexShard = indexService.getShard(0);

        SearchService.SearchRewriteContext rewriteContext = service.acquireSearcherAndRewrite(
            new ShardSearchRequest(shardId, 0, AliasFilter.EMPTY) {
                @Override
                public SearchType searchType() {
                    // induce an artificial NPE
                    throw new NullPointerException("expected");
                }
            }, indexShard);
        NullPointerException e = expectThrows(NullPointerException.class,
            () -> service.createContext(rewriteContext));
        assertEquals("expected", e.getMessage());
        assertEquals("should have 2 store refs (IndexService + InternalEngine)", 2, indexService.getShard(0).store().refCount());
    }

    public void testMatchNoDocsEmptyResponse() throws InterruptedException {
        createIndex("index");
        Thread currentThread = Thread.currentThread();
        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest()
            .allowPartialSearchResults(false)
            .source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.count("count").field("value")));
        ShardSearchRequest shardRequest = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(),
            5, AliasFilter.EMPTY, 1.0f, 0, null, null);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());

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
            client().prepareIndex("test").setSource("f", "v").get();
        }
        client().admin().indices().prepareRefresh("test").get();
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchers = new Thread[randomIntBetween(1, 4)];
        CountDownLatch latch = new CountDownLatch(searchers.length);
        for (int i = 0; i < searchers.length; i++) {
            searchers[i] = new Thread(() -> {
                latch.countDown();
                while (stopped.get() == false) {
                    try {
                        client().prepareSearch("test").setRequestCache(false).get();
                    } catch (Exception ignored) {
                        return;
                    }
                }
            });
            searchers[i].start();
        }
        latch.await();
        client().admin().indices().prepareDelete("test").get();
        stopped.set(true);
        for (Thread searcher : searchers) {
            searcher.join();
        }
    }
}
