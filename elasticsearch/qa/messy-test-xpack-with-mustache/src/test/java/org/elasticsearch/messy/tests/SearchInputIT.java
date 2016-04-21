/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.watcher.input.search.SearchInput;
import org.elasticsearch.watcher.input.search.SearchInputFactory;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

/**
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class SearchInputIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        types.add(MustachePlugin.class);
        return types;
    }

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
        String path = "/org/elasticsearch/watcher/input/search/config/scripts/test_disk_template.mustache";
        try (InputStream stream  = SearchInputIT.class.getResourceAsStream("/org/elasticsearch/watcher/input/search/config/scripts" +
                "/test_disk_template.mustache");
             OutputStream out = Files.newOutputStream(scriptPath.resolve("test_disk_template.mustache"))) {
            Streams.copy(stream, out);
        } catch (IOException e) {
            throw new RuntimeException("failed to copy mustache template");
        }


        //Set path so ScriptService will pick up the test scripts
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", configPath).build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .build();
    }

    public void testExecute() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                        .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}")));
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), null);
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        new WatchStatus(new DateTime(0, UTC), emptyMap())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertEquals(result.executedRequest().searchType(), request.searchType());
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());
    }

    public void testSearchInlineTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        final String expectedTemplateString = "{\"query\":{\"filtered\":{\"query\":{\"match\":{\"event_type\":{\"query\":\"a\","
                + "\"type\":\"boolean\"}}},\"filter\":{\"range\":{\"_timestamp\":"
                + "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\","
                + "\"include_lower\":true,\"include_upper\":true}}}}}}";

        Map<String, Object> triggerParams = new HashMap<String, Object>();
        triggerParams.put("triggered_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        triggerParams.put("scheduled_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        Map<String, Object> ctxParams = new HashMap<String, Object>();
        ctxParams.put("id", ctx.id().value());
        ctxParams.put("metadata", null);
        ctxParams.put("vars", new HashMap<String, Object>());
        ctxParams.put("watch_id", "test-watch");
        ctxParams.put("trigger", triggerParams);
        ctxParams.put("payload", new Payload.Simple().data());
        ctxParams.put("execution_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        Map<String, Object> expectedParams = new HashMap<String, Object>();
        expectedParams.put("seconds_param", "30s");
        expectedParams.put("ctx", ctxParams);
        Template expectedTemplate = new Template(expectedTemplateString, ScriptType.INLINE, null, XContentType.JSON, expectedParams);
        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Template template = new Template(TEMPLATE_QUERY, ScriptType.INLINE, null, XContentType.JSON, params);

        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").setTemplate(template).request();

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        assertThat(executedResult.executedRequest().template(), equalTo(expectedTemplate));
    }

    public void testSearchIndexedTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        PutStoredScriptRequest indexedScriptRequest = client().admin().cluster().preparePutStoredScript()
                .setId("test-template")
                .setScriptLang("mustache")
                .setSource(new BytesArray(TEMPLATE_QUERY))
                .request();
        assertThat(client().admin().cluster().putStoredScript(indexedScriptRequest).actionGet().isAcknowledged(), is(true));

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Template template = new Template("test-template", ScriptType.STORED, null, null, params);

        jsonBuilder().value(TextTemplate.indexed("test-template").params(params).build()).bytes();
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").setTemplate(template).request();

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        Template resultTemplate = executedResult.executedRequest().template();
        assertThat(resultTemplate, notNullValue());
        assertThat(resultTemplate.getScript(), equalTo("test-template"));
        assertThat(resultTemplate.getType(), equalTo(ScriptType.STORED));
    }

    public void testSearchOnDiskTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Template template = new Template("test_disk_template", ScriptType.FILE, null, null, params);
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").setTemplate(template).request();

        SearchInput.Result executedResult = executeSearchInput(request, ctx);
        Template resultTemplate = executedResult.executedRequest().template();
        assertThat(resultTemplate, notNullValue());
        assertThat(resultTemplate.getScript(), equalTo("test_disk_template"));
        assertThat(resultTemplate.getType(), equalTo(ScriptType.FILE));
    }

    public void testDifferentSearchType() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                        .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))
        );
        SearchType searchType = getRandomSupportedSearchType();

        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(searchType)
                .request()
                .source(searchSourceBuilder);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), null);
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<ActionWrapper>()),
                        null,
                        new WatchStatus(new DateTime(0, UTC), emptyMap())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertEquals(result.executedRequest().searchType(), searchType);
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());
    }

    public void testParserValid() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSource()
                        .query(boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                                .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        XContentBuilder builder = jsonBuilder().value(new SearchInput(request, null, timeout, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        IndicesQueriesRegistry indicesQueryRegistry = internalCluster().getInstance(IndicesQueriesRegistry.class);
        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, WatcherClientProxy.of(client()), indicesQueryRegistry,
                                                            null, null);

        SearchInput searchInput = factory.parseInput("_id", parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
        assertThat(searchInput.getTimeout(), equalTo(timeout));
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
                        new WatchStatus(new DateTime(50000, UTC), emptyMap())),
                new DateTime(60000, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(60000, UTC), new DateTime(60000, UTC)),
                timeValueSeconds(5));
    }

    private SearchInput.Result executeSearchInput(SearchRequest request, WatchExecutionContext ctx) throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");
        SearchInput.Builder siBuilder = SearchInput.builder(request);

        SearchInput si = siBuilder.build();

        ExecutableSearchInput searchInput = new ExecutableSearchInput(si, logger, WatcherClientProxy.of(client()), null);
        return searchInput.execute(ctx, new Payload.Simple());
    }

}
