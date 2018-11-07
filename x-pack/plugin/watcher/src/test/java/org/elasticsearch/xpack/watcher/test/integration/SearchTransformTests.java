/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.transform.TransformBuilders;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
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

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, supportsDedicatedMasters = false,
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
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, client(),
                watcherSearchTemplateService(), TimeValue.timeValueMinutes(1));

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);

        Transform.Result result = transform.execute(ctx, Payload.EMPTY);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));

        SearchResponse response = client().prepareSearch("idx").get();
        Payload expectedPayload = new Payload.XContent(response);

        // we need to remove the "took" and "num_reduce_phases" fields from the response
        // as they are the only fields most likely different between the two... we don't
        // really care about these fields, we just want to make sure that the important
        // parts of the response are the same
        Map<String, Object> resultData = result.payload().data();
        resultData.remove("took");
        resultData.remove("num_reduce_phases");
        Map<String, Object> expectedData = expectedPayload.data();
        expectedData.remove("took");
        expectedData.remove("num_reduce_phases");
        assertThat(resultData, equalTo(expectedData));
    }

    @SuppressWarnings("unchecked")
    public void testExecuteFailure() throws Exception {
        index("idx", "type", "1");
        ensureGreen("idx");
        refresh();

        // create a bad request
        WatcherSearchTemplateRequest request = templateRequest(new SearchSourceBuilder().query(
                QueryBuilders.wrapperQuery(BytesReference.bytes(jsonBuilder().startObject()
                    .startObject("_unknown_query_").endObject().endObject()))), "idx");
        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform transform = new ExecutableSearchTransform(searchTransform, logger, client(),
                watcherSearchTemplateService(), TimeValue.timeValueMinutes(1));

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);

        SearchTransform.Result result = transform.execute(ctx, Payload.EMPTY);
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
            TextTemplate template = new TextTemplate(templateName, null, ScriptType.INLINE, null);
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

        final MockScriptEngine engine = new MockScriptEngine("mock", Collections.emptyMap(), Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(engine.getType(), engine);
        ScriptService scriptService = new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);

        SearchTransformFactory transformFactory = new SearchTransformFactory(Settings.EMPTY, client(),  xContentRegistry(), scriptService);
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
                    equalTo(new Script(ScriptType.INLINE, "mustache", "template1", Collections.emptyMap())));
        }
        assertThat(executable.transform().getRequest().getSearchSource().utf8ToString(), equalTo("{\"query\":{\"match_all\":{}}}"));
        assertThat(executable.transform().getTimeout(), equalTo(readTimeout));
    }

    public void testDifferentSearchType() throws Exception {
        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
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

    private SearchTransform.Result executeSearchTransform(WatcherSearchTemplateRequest request, WatchExecutionContext ctx)
            throws IOException {
        createIndex("test-search-index");
        ensureGreen("test-search-index");

        SearchTransform searchTransform = TransformBuilders.searchTransform(request).build();
        ExecutableSearchTransform executableSearchTransform = new ExecutableSearchTransform(searchTransform, logger,
                client(), watcherSearchTemplateService(), TimeValue.timeValueMinutes(1));

        return executableSearchTransform.execute(ctx, Payload.Simple.EMPTY);
    }

    private WatcherSearchTemplateService watcherSearchTemplateService() {
        String master = internalCluster().getMasterName();
        return new WatcherSearchTemplateService(
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
        public List<ScriptContext<?>> getContexts() {
            return Collections.singletonList(Watcher.SCRIPT_TEMPLATE_CONTEXT);
        }
    }
}
