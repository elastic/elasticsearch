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

import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FailOnRewriteQueryPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        static final String DUMMY_SCRIPT = "dummyScript";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(DUMMY_SCRIPT, vars -> {
                return "dummy";
            });
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.default_search_timeout", "5s").build();
    }

    public void testClearOnClose() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }

    public void testCloseSearchContextOnRewriteException() {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

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

    public void testSearchWhileIndexDeleted() throws IOException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

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
                        client().prepareIndex("index", "type").setSource("field", "value")
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
            for (int i = 0; i < rounds; i++) {
                try {
                    SearchPhaseResult searchPhaseResult = service.executeQueryPhase(
                            new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                                    new SearchSourceBuilder(), new String[0], false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f),
                        new SearchTask(123L, "", "", "", null));
                    IntArrayList intCursors = new IntArrayList(1);
                    intCursors.add(0);
                    ShardFetchRequest req = new ShardFetchRequest(searchPhaseResult.getRequestId(), intCursors, null /* not a scroll */);
                    service.executeFetchPhase(req, new SearchTask(123L, "", "", "", null));
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
    }

    public void testTimeout() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchContext contextWithDefaultTimeout = service.createContext(
                new ShardSearchLocalRequest(
                        indexShard.shardId(),
                        1,
                        SearchType.DEFAULT,
                        new SearchSourceBuilder(),
                        new String[0],
                        false,
                        new AliasFilter(null, Strings.EMPTY_ARRAY),
                        1.0f)
        );
        try {
            // the search context should inherit the default timeout
            assertThat(contextWithDefaultTimeout.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        } finally {
            contextWithDefaultTimeout.decRef();
            service.freeContext(contextWithDefaultTimeout.id());
        }

        final long seconds = randomIntBetween(6, 10);
        final SearchContext context = service.createContext(
                new ShardSearchLocalRequest(
                        indexShard.shardId(),
                        1,
                        SearchType.DEFAULT,
                        new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(seconds)),
                        new String[0],
                        false,
                        new AliasFilter(null, Strings.EMPTY_ARRAY),
                        1.0f)
        );
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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // adding the maximum allowed number of docvalue_fields to retrieve
        for (int i = 0; i < indexService.getIndexSettings().getMaxDocvalueFields(); i++) {
            searchSourceBuilder.docValueField("field" + i);
        }
        try (SearchContext context = service.createContext(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                searchSourceBuilder, new String[0], false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f))) {
            assertNotNull(context);
            searchSourceBuilder.docValueField("one_field_too_much");
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                    () -> service.createContext(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                            searchSourceBuilder, new String[0], false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f)));
            assertEquals(
                    "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [100] but was [101]. "
                            + "This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.",
                    ex.getMessage());
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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // adding the maximum allowed number of script_fields to retrieve
        int maxScriptFields = indexService.getIndexSettings().getMaxScriptFields();
        for (int i = 0; i < maxScriptFields; i++) {
            searchSourceBuilder.scriptField("field" + i,
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
        }
        try (SearchContext context = service.createContext(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                searchSourceBuilder, new String[0], false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f))) {
            assertNotNull(context);
            searchSourceBuilder.scriptField("anotherScriptField",
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                    () -> service.createContext(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                            searchSourceBuilder, new String[0], false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f)));
            assertEquals(
                    "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxScriptFields + "] but was ["
                            + (maxScriptFields + 1)
                            + "]. This limit can be set by changing the [index.max_script_fields] index level setting.",
                    ex.getMessage());
        }
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
        protected void doWriteTo(StreamOutput out) throws IOException {
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        }

        @Override
        protected Query doToQuery(QueryShardContext context) throws IOException {
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

    public void testCanMatch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        assertTrue(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH, null,
            Strings.EMPTY_ARRAY, false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));

        assertTrue(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH,
            new SearchSourceBuilder(), Strings.EMPTY_ARRAY, false, new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));

        assertTrue(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH,
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()), Strings.EMPTY_ARRAY, false,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));

        assertTrue(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH,
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test", ValueType.STRING).minDocCount(0)), Strings.EMPTY_ARRAY, false,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));
        assertTrue(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH,
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
                .aggregation(new GlobalAggregationBuilder("test")), Strings.EMPTY_ARRAY, false,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));

        assertFalse(service.canMatch(new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.QUERY_THEN_FETCH,
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder()), Strings.EMPTY_ARRAY, false,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f)));

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
}
