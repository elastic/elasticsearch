/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.EMPTY_PAYLOAD;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, supportsDedicatedMasters = false,
        numDataNodes = 1)
public class SearchTransformTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins());
        plugins.add(CustomScriptContextPlugin.class);
        plugins.add(MockMustacheScriptEngine.TestPlugin.class);
        return plugins;
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

        WatcherSearchTemplateRequest request = templateRequest(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()), "idx");
        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, WatcherClientProxy.of(client()),
                watcherSearchTemplateService(), null);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Transform.Result result = transform.execute(ctx, EMPTY_PAYLOAD);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));

        SearchResponse response = client().prepareSearch("idx").get();
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
        WatcherSearchTemplateRequest request = templateRequest(new SearchSourceBuilder().query(
                QueryBuilders.wrapperQuery(jsonBuilder().startObject()
                .startObject("_unknown_query_").endObject().endObject().bytes())), "idx");
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
            result.executedRequest().toXContent(builder, ToXContent.EMPTY_PARAMS);

            Map<String, Object> map = createParser(builder).map();
            assertThat(map, hasKey("body"));
            assertThat(map.get("body"), instanceOf(Map.class));

            map = (Map<String, Object>) map.get("body");
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
            TextTemplate template = new TextTemplate(templateName, null, ScriptType.FILE, null);
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

        XContentParser parser = createParser(builder);
        parser.nextToken();

        SearchTransformFactory transformFactory = new SearchTransformFactory(Settings.EMPTY, WatcherClientProxy.of(client()),
                                                                             xContentRegistry(), scriptService());
        ExecutableSearchTransform executable = transformFactory.parseExecutable("_id", parser);

        assertThat(executable, notNullValue());
        assertThat(executable.type(), is(SearchTransform.TYPE));
        assertThat(executable.transform().getRequest(), notNullValue());
        if (indices != null) {
            assertThat(executable.transform().getRequest().getIndices(), arrayContainingInAnyOrder(indices));
        }
        if (searchType != null) {
            assertThat(executable.transform().getRequest().getSearchType(), is(searchType));
        }
        if (templateName != null) {
            assertThat(executable.transform().getRequest().getTemplate(),
                    equalTo(new Script(ScriptType.FILE, "mustache", "template1", Collections.emptyMap())));
        }
        assertThat(executable.transform().getRequest().getSearchSource().utf8ToString(), equalTo("{\"query\":{\"match_all\":{}}}"));
        assertThat(executable.transform().getTimeout(), equalTo(readTimeout));
    }

    public void testDifferentSearchType() throws Exception {
        WatchExecutionContext ctx = createContext();
        SearchSourceBuilder searchSourceBuilder = searchSource().query(boolQuery()
              .must(matchQuery("event_type", "a")));
        final SearchType searchType = getRandomSupportedSearchType();

        WatcherSearchTemplateRequest request = templateRequest(searchSourceBuilder, searchType, "test-search-index");
        SearchTransform.Result result = executeSearchTransform(request, ctx);

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertThat(result.executedRequest(), notNullValue());
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertThat(result.executedRequest().getSearchType(), is(searchType));
        assertThat(result.executedRequest().getIndices(), arrayContainingInAnyOrder(request.getIndices()));
        assertThat(result.executedRequest().getIndicesOptions(), equalTo(request.getIndicesOptions()));
    }

    private WatchExecutionContext createContext() {

        return new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        AlwaysCondition.INSTANCE,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        new WatchStatus( new DateTime(40000, UTC), emptyMap())),
                new DateTime(60000, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(60000, UTC), new DateTime(60000, UTC)),
                timeValueSeconds(5));
    }

    private SearchTransform.Result executeSearchTransform(WatcherSearchTemplateRequest request, WatchExecutionContext ctx)
            throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");

        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform executableSearchTransform = new ExecutableSearchTransform(searchTransform, logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);

        return executableSearchTransform.execute(ctx, Payload.Simple.EMPTY);
    }

    private WatcherSearchTemplateService watcherSearchTemplateService() {
        String master = internalCluster().getMasterName();
        return new WatcherSearchTemplateService(internalCluster().clusterService(master).getSettings(),
                internalCluster().getInstance(ScriptService.class, master),
                xContentRegistry()
        );
    }

    private ScriptService scriptService() {
        return internalCluster().getInstance(ScriptService.class);
    }

    /**
     * Custom plugin that registers XPack script context.
     */
    public static class CustomScriptContextPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptContext.Plugin getCustomScriptContexts() {
            return new ScriptContext.Plugin("xpack", "watch");
        }
    }
}
