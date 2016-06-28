/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
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
import org.elasticsearch.xpack.watcher.actions.ExecutableActions;
import org.elasticsearch.xpack.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.Script;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.transform.TransformBuilders;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils.parseDate;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.EMPTY_PAYLOAD;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.simplePayload;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

/**
 *
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, supportsDedicatedMasters = false,
        numDataNodes = 1)
public class SearchTransformIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        types.add(MustachePlugin.class);
        types.add(CustomScriptContextPlugin.class);
        return types;
    }

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
        String path = "/org/elasticsearch/xpack/watcher/transform/search/config/scripts/test_disk_template.mustache";
        try (InputStream stream  = SearchTransformIT.class.getResourceAsStream(path);
             OutputStream out = Files.newOutputStream(scriptPath.resolve("test_disk_template.mustache"))) {
            Streams.copy(stream, out);
        } catch (IOException e) {
            throw new RuntimeException("failed to copy mustache template");
        }
        //Set path so ScriptService will pick up the test scripts
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // we're not extending from the base watcher test case, so we should prevent the watcher plugin from being loaded
                .put("path.conf", configPath).build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                .put(super.indexSettings())

                // we have to test this on an index that has at least 2 shards. Otherwise when searching indices with
                // a single shard the QUERY_THEN_FETCH search type will change to QUERY_AND_FETCH during execution.
                .put("index.number_of_shards", randomIntBetween(2, 5))

                .build();
    }

    public void testExecute() throws Exception {
        index("idx", "type", "1");
        ensureGreen("idx");
        refresh();

        SearchRequest request = Requests.searchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, WatcherClientProxy.of(client()),
                watcherSearchTemplateService(), null);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Transform.Result result = transform.execute(ctx, EMPTY_PAYLOAD);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));

        SearchResponse response = client().search(request).get();
        Payload expectedPayload = new Payload.XContent(response);

        // we need to remove the "took" field from teh response as this is the only field
        // that most likely be different between the two... we don't really care about this
        // field, we just want to make sure that the important parts of the response are the same
        Map<String, Object> resultData = result.payload().data();
        resultData.remove("took");
        Map<String, Object> expectedData = expectedPayload.data();
        expectedData.remove("took");

        assertThat(resultData, equalTo(expectedData));
    }

    @SuppressWarnings("unchecked")
    public void testExecuteFailure() throws Exception {
        index("idx", "type", "1");
        ensureGreen("idx");
        refresh();

        // create a bad request
        SearchRequest request = Requests.searchRequest("idx").source(
                new SearchSourceBuilder().query(QueryBuilders.wrapperQuery(jsonBuilder().startObject()
                        .startObject("_unknown_query_").endObject().endObject().bytes())));
        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, WatcherClientProxy.of(client()),
                watcherSearchTemplateService(), null);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        SearchTransform.Result result = transform.execute(ctx, EMPTY_PAYLOAD);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.FAILURE));
        assertThat(result.reason(), notNullValue());
        assertThat(result.reason(), containsString("no [query] registered for [_unknown_query_]"));

        // extract the base64 encoded query from the template script, path is: query -> wrapper -> query
        try (XContentBuilder builder = jsonBuilder()) {
            result.executedRequest().source().toXContent(builder, ToXContent.EMPTY_PARAMS);

            String jsonQuery = builder.string();
            Map<String, Object> map = XContentFactory.xContent(jsonQuery).createParser(jsonQuery).map();

            assertThat(map, hasKey("query"));
            assertThat(map.get("query"), instanceOf(Map.class));

            map = (Map<String, Object>) map.get("query");
            assertThat(map, hasKey("wrapper"));
            assertThat(map.get("wrapper"), instanceOf(Map.class));

            map = (Map<String, Object>) map.get("wrapper");
            assertThat(map, hasKey("query"));
            assertThat(map.get("query"), instanceOf(String.class));

            String queryAsBase64 = (String) map.get("query");
            String decodedQuery = new String(Base64.getDecoder().decode(queryAsBase64), StandardCharsets.UTF_8);
            assertThat(decodedQuery, containsString("_unknown_query_"));
        }
    }

    public void testExecuteMustacheTemplate() throws Exception {

        // The rational behind this test:
        //
        // - we index 4 documents each one associated with a unique value and each is associated with a day
        // - we build a search transform such that with a filter that
        //   - the date must be after [scheduled_time] variable
        //   - the date must be before [execution_time] variable
        //   - the value must match [payload.value] variable
        // - the variable are set as such:
        //   - scheduled_time = youngest document's date
        //   - fired_time = oldest document's date
        //   - payload.value = val_3
        // - when executed, the variables will be replaced with the scheduled_time, fired_time and the payload.value.
        // - we set all these variables accordingly (the search transform is responsible to populate them)
        // - when replaced correctly, the search should return document 3.
        //
        // we then do a search for document 3, and compare the response to the payload returned by the transform

        index("idx", "type", "1", doc("2015-01-01T00:00:00", "val_1"));
        index("idx", "type", "2", doc("2015-01-02T00:00:00", "val_2"));
        index("idx", "type", "3", doc("2015-01-03T00:00:00", "val_3"));
        index("idx", "type", "4", doc("2015-01-04T00:00:00", "val_4"));

        ensureGreen("idx");
        refresh();

        SearchRequest request = Requests.searchRequest("idx").source(searchSource().query(boolQuery()
                .must(constantScoreQuery(rangeQuery("date").gt("{{ctx.trigger.scheduled_time}}")))
                .must(constantScoreQuery(rangeQuery("date").lt("{{ctx.execution_time}}")))
                .must(termQuery("value", "{{ctx.payload.value}}"))));

        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, WatcherClientProxy.of(client()),
                watcherSearchTemplateService(), null);

        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_name", parseDate("2015-01-04T00:00:00", UTC),
                parseDate("2015-01-01T00:00:00", UTC));
        WatchExecutionContext ctx = mockExecutionContext("_name", parseDate("2015-01-04T00:00:00", UTC), event, EMPTY_PAYLOAD);

        Payload payload = simplePayload("value", "val_3");

        Transform.Result result = transform.execute(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));

        SearchResponse response = client().prepareSearch("idx").setSearchType(ExecutableSearchTransform.DEFAULT_SEARCH_TYPE).setQuery(
                boolQuery()
                        .must(constantScoreQuery(rangeQuery("date").gt(parseDate("2015-01-01T00:00:00", UTC))))
                        .must(constantScoreQuery(rangeQuery("date").lt(parseDate("2015-01-04T00:00:00", UTC))))
                        .must(termQuery("value", "val_3"))
        ).get();
        Payload expectedPayload = new Payload.XContent(response);

        // we need to remove the "took" field from teh response as this is the only field
        // that most likely be different between the two... we don't really care about this
        // field, we just want to make sure that the important parts of the response are the same
        Map<String, Object> resultData = result.payload().data();
        resultData.remove("took");
        Map<String, Object> expectedData = expectedPayload.data();
        expectedData.remove("took");

        assertThat(resultData, equalTo(expectedData));
    }

    public void testParser() throws Exception {
        String[] indices = rarely() ? null : randomBoolean() ? new String[] { "idx" } : new String[] { "idx1", "idx2" };
        SearchType searchType = getRandomSupportedSearchType();
        String templateName = randomBoolean() ? null : "template1";
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject("request");
        if (indices != null) {
            builder.array("indices", indices);
        }
        if (searchType != null) {
            builder.field("search_type", searchType.name());
        }
        if (templateName != null) {
            TextTemplate template = TextTemplate.file(templateName).build();
            builder.field("template", template);
        }

        builder.startObject("body")
                .startObject("query")
                .startObject("match_all")
                .endObject()
                .endObject()
                .endObject();

        builder.endObject();
        TimeValue readTimeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        if (readTimeout != null) {
            builder.field("timeout", readTimeout);
        }
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        IndicesQueriesRegistry indicesQueryRegistry = internalCluster().getInstance(IndicesQueriesRegistry.class);
        SearchTransformFactory transformFactory = new SearchTransformFactory(Settings.EMPTY, WatcherClientProxy.of(client()),
                                                                             indicesQueryRegistry, null, null, scriptService());
        ExecutableSearchTransform executable = transformFactory.parseExecutable("_id", parser);

        assertThat(executable, notNullValue());
        assertThat(executable.type(), is(SearchTransform.TYPE));
        assertThat(executable.transform().getRequest(), notNullValue());
        if (indices != null) {
            assertThat(executable.transform().getRequest().getRequest().indices(), arrayContainingInAnyOrder(indices));
        }
        if (searchType != null) {
            assertThat(executable.transform().getRequest().getRequest().searchType(), is(searchType));
        }
        if (templateName != null) {
            assertThat(executable.transform().getRequest().getTemplate(),
                    equalTo(Script.file("template1").build()));
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        assertThat(executable.transform().getRequest().getRequest().source(), equalTo(source));
        assertThat(executable.transform().getTimeout(), equalTo(readTimeout));
    }

    public void testSearchInlineTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        final String templateQuery = "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"event_type\":{\"query\":\"a\"," +
                "\"type\":\"boolean\"}}},{\"range\":{\"_timestamp\":" +
                "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
                "\"include_lower\":true,\"include_upper\":true}}}]}}}";

        Map<String, Object> triggerParams = new HashMap<String, Object>();
        triggerParams.put("triggered_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        triggerParams.put("scheduled_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        Map<String, Object> ctxParams = new HashMap<String, Object>();
        ctxParams.put("id", ctx.id().value());
        ctxParams.put("metadata", null);
        ctxParams.put("vars", new HashMap<String, Object>());
        ctxParams.put("watch_id", "test-watch");
        ctxParams.put("payload", new HashMap<String, Object>());
        ctxParams.put("trigger", triggerParams);
        ctxParams.put("execution_time", new DateTime(1970, 01, 01, 00, 01, 00, 000, ISOChronology.getInstanceUTC()));
        Map<String, Object> expectedParams = new HashMap<String, Object>();
        expectedParams.put("seconds_param", "30s");
        expectedParams.put("ctx", ctxParams);

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Script template = Script.inline(templateQuery).lang("mustache").params(params).build();
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchTransform.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").request();

        SearchTransform.Result executedResult = executeSearchTransform(request, template, ctx);

        assertThat(executedResult.status(), is(Transform.Result.Status.SUCCESS));
        assertEquals(executedResult.executedRequest().searchType(), request.searchType());
        assertArrayEquals(executedResult.executedRequest().indices(), request.indices());
        assertEquals(executedResult.executedRequest().indicesOptions(), request.indicesOptions());

        XContentSource source = toXContentSource(executedResult);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));
    }

    public void testSearchIndexedTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        final String templateQuery = "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"event_type\":{\"query\":\"a\"," +
                "\"type\":\"boolean\"}}},{\"range\":{\"_timestamp\":" +
                "{\"from\":\"{{ctx.trigger.scheduled_time}}||-{{seconds_param}}\",\"to\":\"{{ctx.trigger.scheduled_time}}\"," +
                "\"include_lower\":true,\"include_upper\":true}}}]}}}";

        PutStoredScriptRequest indexedScriptRequest = client().admin().cluster().preparePutStoredScript()
                .setId("test-script")
                .setScriptLang("mustache")
                .setSource(new BytesArray(templateQuery))
                .request();
        assertThat(client().admin().cluster().putStoredScript(indexedScriptRequest).actionGet().isAcknowledged(), is(true));

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Script template = Script.indexed("test-script").lang("mustache").params(params).build();

        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(ExecutableSearchTransform.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index")
                .request();

        SearchTransform.Result result = executeSearchTransform(request, template, ctx);

        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());

        XContentSource source = toXContentSource(result);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));
    }

    public void testSearchOnDiskTemplate() throws Exception {
        WatchExecutionContext ctx = createContext();

        Map<String, Object> params = new HashMap<>();
        params.put("seconds_param", "30s");

        Script template = Script.file("test_disk_template").lang("mustache").params(params).build();
        SearchRequest request = client().prepareSearch().setSearchType(ExecutableSearchTransform.DEFAULT_SEARCH_TYPE)
                .setIndices("test-search-index").request();

        SearchTransform.Result result = executeSearchTransform(request, template, ctx);

        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertArrayEquals(result.executedRequest().indices(), request.indices());
        assertEquals(result.executedRequest().indicesOptions(), request.indicesOptions());

        XContentSource source = toXContentSource(result);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));
    }

    public void testDifferentSearchType() throws Exception {
        WatchExecutionContext ctx = createContext();

        SearchSourceBuilder searchSourceBuilder = searchSource().query(boolQuery()
              .must(matchQuery("event_type", "a"))
              .must(rangeQuery("_timestamp")
                      .from("{{ctx.trigger.scheduled_time}}||-30s")
                      .to("{{ctx.trigger.triggered_time}}")));

        final SearchType searchType = getRandomSupportedSearchType();
        SearchRequest request = client()
                .prepareSearch("test-search-index")
                .setSearchType(searchType)
                .request()
                .source(searchSourceBuilder);

        SearchTransform.Result result = executeSearchTransform(request, null, ctx);

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertThat(result.executedRequest(), notNullValue());
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertThat(result.executedRequest().searchType(), is(searchType));
        assertThat(result.executedRequest().indices(), arrayContainingInAnyOrder(request.indices()));
        assertThat(result.executedRequest().indicesOptions(), equalTo(request.indicesOptions()));

        XContentSource source = toXContentSource(result);
        assertThat(source.getValue("query.bool.must.1.range._timestamp.from"), equalTo("1970-01-01T00:01:00.000Z||-30s"));
        assertThat(source.getValue("query.bool.must.1.range._timestamp.to"), equalTo("1970-01-01T00:01:00.000Z"));
    }

    private WatchExecutionContext createContext() {

        return new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        new ExecutableAlwaysCondition(logger),
                        null,
                        null,
                        new ExecutableActions(new ArrayList<>()),
                        null,
                        new WatchStatus( new DateTime(40000, UTC), emptyMap())),
                new DateTime(60000, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(60000, UTC), new DateTime(60000, UTC)),
                timeValueSeconds(5));
    }

    private SearchTransform.Result executeSearchTransform(SearchRequest request, Script template, WatchExecutionContext ctx)
            throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");

        SearchTransform searchTransform = TransformBuilders.searchTransform(new WatcherSearchTemplateRequest(request, template)).build();
        ExecutableSearchTransform executableSearchTransform = new ExecutableSearchTransform(searchTransform, logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);

        return executableSearchTransform.execute(ctx, Payload.Simple.EMPTY);
    }

    protected WatcherSearchTemplateService watcherSearchTemplateService() {
        String master = internalCluster().getMasterName();
        return new WatcherSearchTemplateService(internalCluster().clusterService(master).getSettings(),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class, master), internalCluster().clusterService(master)),
                internalCluster().getInstance(IndicesQueriesRegistry.class, master),
                internalCluster().getInstance(AggregatorParsers.class, master),
                internalCluster().getInstance(Suggesters.class, master)
        );
    }

    protected ScriptServiceProxy scriptService() {
        return ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class), internalCluster().clusterService());
    }

    private static Map<String, Object> doc(String date, String value) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("date", parseDate(date, UTC));
        doc.put("value", value);
        return doc;
    }

    private XContentSource toXContentSource(SearchTransform.Result result) throws IOException {
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
