/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.watcher.test.WatcherTestUtils.areJsonEquivalent;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;


/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class SearchInputTests extends ElasticsearchIntegrationTest {

    private final static String TEMPLATE_QUERY = "{\"query\":{\"filtered\":{\"query\":{\"match\":{\"event_type\":{\"query\":\"a\"," +
            "\"type\":\"boolean\"}}},\"filter\":{\"range\":{\"_timestamp\":" +
            "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
            "\"include_lower\":true,\"include_upper\":true}}}}}}";

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        //Set path so ScriptService will pick up the test scripts
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", this.getResource("config").getPath()).build();
    }

    @Test
    public void testExecute() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}")));
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null), logger, ClientProxy.of(client()));
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ClockMock(),
                        mock(LicenseService.class),
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        null,
                        new Watch.Status()),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)));
        SearchInput.Result result = searchInput.execute(ctx);

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertEquals(result.executedRequest().searchType(),request.searchType());
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());
    }

    @Test
    public void testSearch_InlineTemplate() throws Exception {
        final String expectedQuery = "{\"template\":{\"query\":{\"filtered\":{\"query\":{\"match\":{\"event_type\":{\"query\":\"a\"," +
                "\"type\":\"boolean\"}}},\"filter\":{\"range\":{\"_timestamp\":" +
                "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
                "\"include_lower\":true,\"include_upper\":true}}}}}},\"params\":{\"seconds_param\":\"30s\",\"ctx\":{\"metadata\":null,\"watch_id\":\"test-watch\",\"trigger\":{\"triggered_time\":\"1970-01-01T00:01:00.000Z\",\"scheduled_time\":\"1970-01-01T00:01:00.000Z\"},\"execution_time\":\"1970-01-01T00:01:00.000Z\"}}}";

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        BytesReference templateSource = jsonBuilder()
                .value(Template.inline(TEMPLATE_QUERY).params(params).build())
                .bytes();
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index")
                .setTemplateSource(templateSource)
                .request();


        SearchInput.Result executedResult = executeSearchInput(request);
        assertThat(areJsonEquivalent(executedResult.executedRequest().templateSource().toUtf8(), expectedQuery), is(true));
    }

    @Test
    public void testSearch_IndexedTemplate() throws Exception {
        PutIndexedScriptRequest indexedScriptRequest = client().preparePutIndexedScript("mustache","test-template", TEMPLATE_QUERY).request();
        assertThat(client().putIndexedScript(indexedScriptRequest).actionGet().isCreated(), is(true));

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        BytesReference templateSource = jsonBuilder()
                .value(Template.indexed("test-template").params(params).build())
                .bytes();
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index")
                .setTemplateSource(templateSource)
                .request();

        SearchInput.Result executedResult = executeSearchInput(request);
        assertThat(executedResult.executedRequest().templateSource().toUtf8(), startsWith("{\"template\":{\"id\":\"test-template\""));
    }

    @Test
    public void testSearch_OndiskTemplate() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        BytesReference templateSource = jsonBuilder()
                .value(Template.file("test_disk_template").params(params).build())
                .bytes();
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index")
                .setTemplateSource(templateSource)
                .request();

        SearchInput.Result executedResult = executeSearchInput(request);
        assertThat(executedResult.executedRequest().templateSource().toUtf8(), startsWith("{\"template\":{\"file\":\"test_disk_template\""));
    }

    @Test
    public void testDifferentSearchType() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))
        );
        SearchType searchType = getRandomSupportedSearchType();

        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(searchType)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null), logger, ClientProxy.of(client()));
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ClockMock(),
                        mock(LicenseService.class),
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        null,
                        new Watch.Status()),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)));
        SearchInput.Result result = searchInput.execute(ctx);

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertEquals(result.executedRequest().searchType(), searchType);
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());
    }

    @Test
    public void testParser_Valid() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSource()
                        .query(filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        XContentBuilder builder = jsonBuilder().value(new SearchInput(request, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()));

        SearchInput searchInput = factory.parseInput("_id", parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
    }

    @Test(expected = SearchInputException.class)
    public void testParser_ScanNotSupported() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .request()
                .source(searchSource()
                        .query(filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        XContentBuilder builder = jsonBuilder().value(new SearchInput(request, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()));

        factory.parseInput("_id", parser);
        fail("expected a SearchInputException as search type SCAN should not be supported");
    }

    @Test(expected = SearchInputException.class)
    public void testParser_Invalid() throws Exception {
        SearchInputFactory factory = new SearchInputFactory(settingsBuilder().build(), ClientProxy.of(client()));

        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", new ArrayList<String>());

        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(SearchInput.Field.PAYLOAD.getPreferredName(), data);
        jsonBuilder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        factory.parseResult("_id", parser);
        fail("result parsing should fail if payload is provided but request is missing");
    }

    @Test
    public void testResultParser() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", new ArrayList<String>() );

        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{ctx.triggered.scheduled_time}}||-30s").to("{{ctx.triggered.triggered_time}}")));
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(SearchInput.Field.PAYLOAD.getPreferredName(), data);
        jsonBuilder.field(SearchInput.Field.REQUEST.getPreferredName());
        WatcherUtils.writeSearchRequest(request, jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();

        SearchInputFactory factory = new SearchInputFactory(settingsBuilder().build(), ClientProxy.of(client()));

        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        SearchInput.Result result = factory.parseResult("_id", parser);

        assertEquals(SearchInput.TYPE, result.type());
        assertEquals(result.payload().data().get("foo"), "bar");
        List baz = (List)result.payload().data().get("baz");
        assertTrue(baz.isEmpty());
        assertNotNull(result.executedRequest());
    }

    private SearchInput.Result executeSearchInput(SearchRequest request) throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");
        SearchInput.Builder siBuilder = SearchInput.builder(request);

        SearchInput si = siBuilder.build();

        ExecutableSearchInput searchInput = new ExecutableSearchInput(si, logger, ClientProxy.of(client()));
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ClockMock(),
                        mock(LicenseService.class),
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        null,
                        new Watch.Status()),
                new DateTime(60000, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(60000, UTC), new DateTime(60000, UTC)));
        return searchInput.execute(ctx);
    }

}
