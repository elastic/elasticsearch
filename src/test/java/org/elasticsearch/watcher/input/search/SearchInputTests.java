/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.DynamicIndexName;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.watcher.test.WatcherTestUtils.areJsonEquivalent;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.*;
import static org.joda.time.DateTimeZone.UTC;

/**
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class SearchInputTests extends ElasticsearchIntegrationTest {

    private final static String TEMPLATE_QUERY = "{\"query\":{\"filtered\":{\"query\":{\"match\":{\"event_type\":{\"query\":\"a\"," +
            "\"type\":\"boolean\"}}},\"filter\":{\"range\":{\"_timestamp\":" +
            "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
            "\"include_lower\":true,\"include_upper\":true}}}}}}";

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        final Path tempDir = createTempDir();
        final Path configPath = tempDir.resolve("config");
        final Path scriptPath = configPath.resolve("scripts");
        try {
            Files.createDirectories(scriptPath);
        } catch (IOException e) {
            throw new RuntimeException("failed to create config dir");

        }
        try (InputStream stream  = SearchInputTests.class.getResourceAsStream("/org/elasticsearch/watcher/input/search/config/scripts/test_disk_template.mustache");
            OutputStream out = Files.newOutputStream(scriptPath.resolve("test_disk_template.mustache"))) {
            Streams.copy(stream, out);
        } catch (IOException e) {
            throw new RuntimeException("failed to copy mustache template");
        }


        //Set path so ScriptService will pick up the test scripts
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put(PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .put("path.conf", configPath).build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .build();
    }

    @Test
    public void testExecute() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}")));
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger, ClientProxy.of(client()), null, new DynamicIndexName.Parser());
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        new WatchStatus(ImmutableMap.<String, ActionStatus>of())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
        SearchInput.Result result = searchInput.execute(ctx);

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertEquals(result.executedRequest().searchType(), request.searchType());
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());
    }

    @Test
    public void testSearch_InlineTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        final String expectedQuery = "{\"template\":{\"query\":{\"filtered\":{\"query\":{\"match\":{\"event_type\":{\"query\":\"a\"," +
                "\"type\":\"boolean\"}}},\"filter\":{\"range\":{\"_timestamp\":" +
                "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
                "\"include_lower\":true,\"include_upper\":true}}}}}},\"params\":{\"seconds_param\":\"30s\",\"ctx\":{\"id\":\"" + ctx.id().value() + "\",\"metadata\":null,\"vars\":{},\"watch_id\":\"test-watch\",\"trigger\":{\"triggered_time\":\"1970-01-01T00:01:00.000Z\",\"scheduled_time\":\"1970-01-01T00:01:00.000Z\"},\"execution_time\":\"1970-01-01T00:01:00.000Z\"}}}";

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

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        assertThat(areJsonEquivalent(executedResult.executedRequest().templateSource().toUtf8(), expectedQuery), is(true));
    }

    @Test
    public void testSearch_IndexedTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

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

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        assertThat(executedResult.executedRequest().templateSource().toUtf8(), startsWith("{\"template\":{\"id\":\"test-template\""));
    }

    @Test
    public void testSearch_OndiskTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

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

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        assertThat(executedResult.executedRequest().templateSource().toUtf8(), startsWith("{\"template\":{\"file\":\"test_disk_template\""));
    }

    @Test
    public void testDifferentSearchType() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))
        );
        SearchType searchType = getRandomSupportedSearchType();

        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(searchType)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger, ClientProxy.of(client()), null, new DynamicIndexName.Parser());
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        new WatchStatus(ImmutableMap.<String, ActionStatus>of())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
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
                        .query(filteredQuery(matchQuery("event_type", "a"), rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        XContentBuilder builder = jsonBuilder().value(new SearchInput(request, null, timeout, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, ClientProxy.of(client()));

        SearchInput searchInput = factory.parseInput("_id", parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
        assertThat(searchInput.getTimeout(), equalTo(timeout));
    }

    @Test
    public void testParser_IndexNames() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test", "<test-{now/d-1d}>")
                .request()
                .source(searchSource()
                        .query(boolQuery().must(matchQuery("event_type", "a")).filter(rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        DateTime now = DateTime.now(UTC);
        DateTimeZone timeZone = randomBoolean() ? DateTimeZone.forOffsetHours(-2) : null;
        if (timeZone != null) {
            now = now.withHourOfDay(0).withMinuteOfHour(0);
        }

        boolean timeZoneInWatch = randomBoolean();
        SearchInput input = timeZone != null && timeZoneInWatch ?
                new SearchInput(request, null, null, timeZone) :
                new SearchInput(request, null, null, null);

        XContentBuilder builder = jsonBuilder().value(input);
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        String dateFormat;
        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            dateFormat = DynamicIndexName.DEFAULT_DATE_FORMAT;
        } else {
            dateFormat = "YYYY-MM-dd";
            settingsBuilder.put("watcher.input.search.dynamic_indices.default_date_format", dateFormat);
        }
        if (timeZone != null && !timeZoneInWatch) {
            settingsBuilder.put("watcher.input.search.dynamic_indices.time_zone", timeZone);
        }

        SearchInputFactory factory = new SearchInputFactory(settingsBuilder.build(), ClientProxy.of(client()));

        ExecutableSearchInput executable = factory.parseExecutable("_id", parser);
        DynamicIndexName[] indexNames = executable.indexNames();
        assertThat(indexNames, notNullValue());

        String[] names = DynamicIndexName.names(indexNames, now);
        assertThat(names, notNullValue());
        assertThat(names.length, is(2));
        if (timeZone != null) {
            now = now.withZone(timeZone);
        }
        assertThat(names, arrayContaining("test", "test-" + DateTimeFormat.forPattern(dateFormat).print(now.minusDays(1))));
    }

    @Test(expected = SearchInputException.class)
    public void testParser_ScanNotSupported() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .request()
                .source(searchSource()
                        .query(filteredQuery(matchQuery("event_type", "a"), rangeQuery("_timestamp").from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        XContentBuilder builder = jsonBuilder().value(new SearchInput(request, null, null, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, ClientProxy.of(client()));

        factory.parseInput("_id", parser);
        fail("expected a SearchInputException as search type SCAN should not be supported");
    }

    private WatchExecutionContext createContext() {
        return new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        new WatchStatus(ImmutableMap.<String, ActionStatus>of())),
                new DateTime(60000, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(60000, UTC), new DateTime(60000, UTC)),
                timeValueSeconds(5));
    }

    private SearchInput.Result executeSearchInput(SearchRequest request, WatchExecutionContext ctx) throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");
        SearchInput.Builder siBuilder = SearchInput.builder(request);

        SearchInput si = siBuilder.build();

        ExecutableSearchInput searchInput = new ExecutableSearchInput(si, logger, ClientProxy.of(client()), null, new DynamicIndexName.Parser());
        return searchInput.execute(ctx);
    }

}
