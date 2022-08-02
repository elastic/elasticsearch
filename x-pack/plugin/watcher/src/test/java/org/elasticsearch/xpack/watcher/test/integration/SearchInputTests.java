/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.transform.script.WatcherTransformScript;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchInputTests extends ESTestCase {

    private ScriptService scriptService;
    private Client client;

    @Before
    public void setup() {
        Map<String, ScriptEngine> engines = new HashMap<>();
        engines.put(MockMustacheScriptEngine.NAME, new MockMustacheScriptEngine());
        Map<String, ScriptContext<?>> contexts = new HashMap<>();
        contexts.put(Watcher.SCRIPT_TEMPLATE_CONTEXT.name, Watcher.SCRIPT_TEMPLATE_CONTEXT);
        contexts.put(WatcherTransformScript.CONTEXT.name, WatcherTransformScript.CONTEXT);
        scriptService = new ScriptService(Settings.EMPTY, engines, contexts, () -> 1L);

        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testExecute() throws Exception {
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        PlainActionFuture<SearchResponse> searchFuture = PlainActionFuture.newFuture();
        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
            "",
            1,
            1,
            0,
            1234,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        searchFuture.onResponse(searchResponse);
        when(client.search(requestCaptor.capture())).thenReturn(searchFuture);

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Map> headersCaptor = ArgumentCaptor.forClass(Map.class);
        when(client.filterWithHeader(headersCaptor.capture())).thenReturn(client);

        SearchSourceBuilder searchSourceBuilder = searchSource().query(boolQuery().must(matchQuery("event_type", "a")));

        WatcherSearchTemplateRequest request = WatcherTestUtils.templateRequest(searchSourceBuilder);
        ExecutableSearchInput searchInput = new ExecutableSearchInput(
            new SearchInput(request, null, null, null),
            client,
            watcherSearchTemplateService(),
            TimeValue.timeValueMinutes(1)
        );
        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();

        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        SearchRequest searchRequest = requestCaptor.getValue();
        assertThat(searchRequest.searchType(), is(request.getSearchType()));
        assertThat(searchRequest.indicesOptions(), is(request.getIndicesOptions()));
        assertThat(searchRequest.indices(), is(arrayContainingInAnyOrder(request.getIndices())));
        assertThat(headersCaptor.getAllValues(), hasSize(0));
    }

    public void testDifferentSearchType() throws Exception {
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        PlainActionFuture<SearchResponse> searchFuture = PlainActionFuture.newFuture();
        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
            "",
            1,
            1,
            0,
            1234,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        searchFuture.onResponse(searchResponse);
        when(client.search(requestCaptor.capture())).thenReturn(searchFuture);

        SearchSourceBuilder searchSourceBuilder = searchSource().query(boolQuery().must(matchQuery("event_type", "a")));
        SearchType searchType = getRandomSupportedSearchType();
        WatcherSearchTemplateRequest request = WatcherTestUtils.templateRequest(searchSourceBuilder, searchType);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(
            new SearchInput(request, null, null, null),
            client,
            watcherSearchTemplateService(),
            TimeValue.timeValueMinutes(1)
        );
        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        SearchRequest searchRequest = requestCaptor.getValue();
        assertThat(searchRequest.searchType(), is(request.getSearchType()));
        assertThat(searchRequest.indicesOptions(), is(request.getIndicesOptions()));
        assertThat(searchRequest.indices(), is(arrayContainingInAnyOrder(request.getIndices())));
    }

    public void testParserValid() throws Exception {
        SearchSourceBuilder source = searchSource().query(
            boolQuery().must(matchQuery("event_type", "a"))
                .must(rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))
        );

        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        XContentBuilder builder = jsonBuilder().value(new SearchInput(WatcherTestUtils.templateRequest(source), null, timeout, null));
        XContentParser parser = createParser(builder);
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, client, xContentRegistry(), scriptService);

        SearchInput searchInput = factory.parseInput("_id", parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
        assertThat(searchInput.getTimeout(), equalTo(timeout));
    }

    // source: https://discuss.elastic.co/t/need-help-for-energy-monitoring-system-alerts/89415/3
    public void testThatEmptyRequestBodyWorks() throws Exception {
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        PlainActionFuture<SearchResponse> searchFuture = PlainActionFuture.newFuture();
        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
            "",
            1,
            1,
            0,
            1234,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        searchFuture.onResponse(searchResponse);
        when(client.search(requestCaptor.capture())).thenReturn(searchFuture);

        try (
            XContentBuilder builder = jsonBuilder().startObject()
                .startObject("request")
                .startArray("indices")
                .value("foo")
                .endArray()
                .endObject()
                .endObject();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {

            parser.nextToken(); // advance past the first starting object

            SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, client, xContentRegistry(), scriptService);
            SearchInput input = factory.parseInput("my-watch", parser);
            assertThat(input.getRequest(), is(not(nullValue())));
            assertThat(input.getRequest().getSearchSource(), is(BytesArray.EMPTY));

            ExecutableSearchInput executableSearchInput = factory.createExecutable(input);
            WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
            SearchInput.Result result = executableSearchInput.execute(ctx, Payload.Simple.EMPTY);
            assertThat(result.status(), is(Input.Result.Status.SUCCESS));
            // no body in the search request
            ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));
            assertThat(requestCaptor.getValue().source().toString(params), is("{}"));
        }
    }

    private WatcherSearchTemplateService watcherSearchTemplateService() {
        SearchModule module = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new WatcherSearchTemplateService(scriptService, new NamedXContentRegistry(module.getNamedXContents()));
    }
}
