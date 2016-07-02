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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.common.ScriptServiceProxy;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.ExecutableActions;
import org.elasticsearch.xpack.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.Script;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
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
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;

/**
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, supportsDedicatedMasters = false,
        numDataNodes = 1)
public class SearchInputIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        types.add(MustachePlugin.class);
        types.add(CustomScriptContextPlugin.class);
        return types;
    }

    private static final String TEMPLATE_QUERY = "{\"query\":{\"bool\":{\"must\":{\"match\":{\"event_type\":{\"query\":\"a\"," +
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
        try (InputStream stream  = SearchInputIT.class.getResourceAsStream("/org/elasticsearch/xpack/watcher/input/search/config/scripts" +
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
        SearchRequest searchRequest = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        WatcherSearchTemplateRequest request = new WatcherSearchTemplateRequest(searchRequest);
        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);
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

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        assertEquals(result.executedRequest().searchType(), request.getRequest().searchType());
        assertArrayEquals(result.executedRequest().indices(), request.getRequest().indices());
        assertEquals(result.executedRequest().indicesOptions(), request.getRequest().indicesOptions());

        XContentSource source = toXContentSource(result);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:00:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:00:00.000Z"));
    }

    public void testSearchInlineTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

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
        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Script template = Script.inline(TEMPLATE_QUERY).lang("mustache").params(params).build();

        SearchRequest request = client().prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").request();

        SearchInput.Result executedResult = executeSearchInput(request, template, ctx);

        assertNotNull(executedResult.executedRequest());
        assertThat(executedResult.status(), is(Input.Result.Status.SUCCESS));
        if (getNumShards("test-search-index").numPrimaries > 1) {
            assertEquals(executedResult.executedRequest().searchType(), request.searchType());
        }
        assertArrayEquals(executedResult.executedRequest().indices(), request.indices());
        assertEquals(executedResult.executedRequest().indicesOptions(), request.indicesOptions());

        XContentSource source = toXContentSource(executedResult);
        assertThat(source.getValue("query.bool.filter.0.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.filter.0.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));
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

        Script template = Script.indexed("test-template").lang("mustache").params(params).build();

        jsonBuilder().value(TextTemplate.indexed("test-template").params(params).build()).bytes();
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").request();

        SearchInput.Result executedResult = executeSearchInput(request, template, ctx);

        assertNotNull(executedResult.executedRequest());
        assertThat(executedResult.status(), is(Input.Result.Status.SUCCESS));
        if (getNumShards("test-search-index").numPrimaries > 1) {
            assertEquals(executedResult.executedRequest().searchType(), request.searchType());
        }
        assertArrayEquals(executedResult.executedRequest().indices(), request.indices());
        assertEquals(executedResult.executedRequest().indicesOptions(), request.indicesOptions());

        XContentSource source = toXContentSource(executedResult);
        assertThat(source.getValue("query.bool.filter.0.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.filter.0.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));

    }

    public void testSearchOnDiskTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Script template = Script.file("test_disk_template").lang("mustache").params(params).build();
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").request();

        SearchInput.Result executedResult = executeSearchInput(request, template, ctx);

        assertNotNull(executedResult.executedRequest());
        assertThat(executedResult.status(), is(Input.Result.Status.SUCCESS));
        assertArrayEquals(executedResult.executedRequest().indices(), request.indices());
        assertEquals(executedResult.executedRequest().indicesOptions(), request.indicesOptions());
    }

    public void testDifferentSearchType() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                        .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))
        );
        SearchType searchType = getRandomSupportedSearchType();

        SearchRequest searchRequest = client()
                .prepareSearch()
                .setSearchType(searchType)
                .request()
                .source(searchSourceBuilder);

        WatcherSearchTemplateRequest request = new WatcherSearchTemplateRequest(searchRequest);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);
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

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        assertEquals(result.executedRequest().searchType(), searchType);
        assertArrayEquals(result.executedRequest().indices(), searchRequest.indices());
        assertEquals(result.executedRequest().indicesOptions(), searchRequest.indicesOptions());

        XContentSource source = toXContentSource(result);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:00:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:00:00.000Z"));
    }

    public void testParserValid() throws Exception {
        SearchRequest searchRequest = client().prepareSearch()
                .setSearchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSource()
                        .query(boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                                .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}"))));

        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        XContentBuilder builder = jsonBuilder().value(
                new SearchInput(new WatcherSearchTemplateRequest(searchRequest), null, timeout, null));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        IndicesQueriesRegistry indicesQueryRegistry = internalCluster().getInstance(IndicesQueriesRegistry.class);
        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, WatcherClientProxy.of(client()), indicesQueryRegistry,
                                                            null, null, scriptService());

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

    private SearchInput.Result executeSearchInput(SearchRequest request, Script template, WatchExecutionContext ctx) throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");
        SearchInput.Builder siBuilder = SearchInput.builder(new WatcherSearchTemplateRequest(request, template));

        SearchInput si = siBuilder.build();

        ExecutableSearchInput searchInput = new ExecutableSearchInput(si, logger, WatcherClientProxy.of(client()),
                watcherSearchTemplateService(), null);
        return searchInput.execute(ctx, new Payload.Simple());
    }

    protected WatcherSearchTemplateService watcherSearchTemplateService() {
        String master = internalCluster().getMasterName();
        return new WatcherSearchTemplateService(internalCluster().clusterService(master).getSettings(),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class, master)),
                internalCluster().getInstance(IndicesQueriesRegistry.class, master),
                internalCluster().getInstance(AggregatorParsers.class, master),
                internalCluster().getInstance(Suggesters.class, master)
        );
    }

    protected ScriptServiceProxy scriptService() {
        return ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class));
    }

    private XContentSource toXContentSource(SearchInput.Result result) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            result.executedRequest().source().toXContent(builder, ToXContent.EMPTY_PARAMS);
            return new XContentSource(builder);
        }
    }

    /**
     * Custom plugin that registers XPack script context.
     */
    public static class CustomScriptContextPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptContext.Plugin getCustomScriptContexts() {
            return ScriptServiceProxy.INSTANCE;
        }
    }
}
