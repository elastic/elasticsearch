/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.chainTransform;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class TransformIntegrationTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.pluginTypes(), CustomScriptPlugin.class);
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        final Path config = createTempDir().resolve("config");
        final Path scripts = config.resolve("scripts");

        try {
            Files.createDirectories(scripts);

            // When using the MockScriptPlugin we can map File scripts to inline scripts:
            // the name of the file script is used in test method while the source of the file script
            // must match a predefined script from CustomScriptPlugin.pluginScripts() method
            Files.write(scripts.resolve("my-script.mockscript"), "['key3' : ctx.payload.key1 + ctx.payload.key2]".getBytes("UTF-8"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        return config;
    }

    public static class CustomScriptPlugin extends WatcherMockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("['key3' : ctx.payload.key1 + ctx.payload.key2]", vars -> {
                int key1 = (int) XContentMapValues.extractValue("ctx.payload.key1", vars);
                int key2 = (int) XContentMapValues.extractValue("ctx.payload.key2", vars);
                return singletonMap("key3", key1 + key2);
            });

            scripts.put("['key4' : ctx.payload.key3 + 10]", vars -> {
                int key3 = (int) XContentMapValues.extractValue("ctx.payload.key3", vars);
                return singletonMap("key4", key3 + 10);
            });

            return scripts;
        }
    }

    public void testScriptTransform() throws Exception {
        final Script script;
        if (randomBoolean()) {
            logger.info("testing script transform with an inline script");
            script = mockScript("['key3' : ctx.payload.key1 + ctx.payload.key2]");
        } else {
            logger.info("testing script transform with an indexed script");
            assertAcked(client().admin().cluster().preparePutStoredScript().setId("my-script").setContent(new BytesArray("""
                {
                  "script": {
                    "lang": "%s",
                    "source": "['key3' : ctx.payload.key1 + ctx.payload.key2]"
                  }
                }""".formatted(MockScriptPlugin.NAME)), XContentType.JSON).get());
            script = new Script(ScriptType.STORED, null, "my-script", Collections.emptyMap());
        }

        // put a watch that has watch level transform:
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id1").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput(MapBuilder.<String, Object>newMapBuilder().put("key1", 10).put("key2", 10)))
                .transform(scriptTransform(script))
                .addAction("_id", indexAction("output1"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));
        // put a watch that has a action level transform:
        putWatchResponse = new PutWatchRequestBuilder(client(), "_id2").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput(MapBuilder.<String, Object>newMapBuilder().put("key1", 10).put("key2", 10)))
                .addAction("_id", scriptTransform(script), indexAction("output2"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));

        executeWatch("_id1");
        executeWatch("_id2");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id1", 1, false);
        assertWatchWithMinimumPerformedActionsCount("_id2", 1, false);
        refresh();

        SearchResponse response = client().prepareSearch("output1").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("key3").toString(), equalTo("20"));

        response = client().prepareSearch("output2").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("key3").toString(), equalTo("20"));
    }

    public void testSearchTransform() throws Exception {
        createIndex("my-condition-index", "my-payload-index");
        ensureGreen("my-condition-index", "my-payload-index");

        indexDoc("my-payload-index", "mytestresult");
        refresh();

        WatcherSearchTemplateRequest inputRequest = templateRequest(searchSource().query(matchAllQuery()), "my-condition-index");
        WatcherSearchTemplateRequest transformRequest = templateRequest(searchSource().query(matchAllQuery()), "my-payload-index");

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id1").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(searchInput(inputRequest))
                .transform(searchTransform(transformRequest))
                .addAction("_id", indexAction("output1"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));
        putWatchResponse = new PutWatchRequestBuilder(client(), "_id2").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(searchInput(inputRequest))
                .addAction("_id", searchTransform(transformRequest), indexAction("output2"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));

        executeWatch("_id1");
        executeWatch("_id2");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id1", 1, false);
        assertWatchWithMinimumPerformedActionsCount("_id2", 1, false);
        refresh();

        SearchResponse response = client().prepareSearch("output1").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("mytestresult"));

        response = client().prepareSearch("output2").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("mytestresult"));
    }

    public void testChainTransform() throws Exception {
        Script script1 = mockScript("['key3' : ctx.payload.key1 + ctx.payload.key2]");
        Script script2 = mockScript("['key4' : ctx.payload.key3 + 10]");

        // put a watch that has watch level transform:
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id1").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput(MapBuilder.<String, Object>newMapBuilder().put("key1", 10).put("key2", 10)))
                .transform(chainTransform(scriptTransform(script1), scriptTransform(script2)))
                .addAction("_id", indexAction("output1"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));
        // put a watch that has a action level transform:
        putWatchResponse = new PutWatchRequestBuilder(client(), "_id2").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput(MapBuilder.<String, Object>newMapBuilder().put("key1", 10).put("key2", 10)))
                .addAction("_id", chainTransform(scriptTransform(script1), scriptTransform(script2)), indexAction("output2"))
        ).get();
        assertThat(putWatchResponse.isCreated(), is(true));

        executeWatch("_id1");
        executeWatch("_id2");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id1", 1, false);
        assertWatchWithMinimumPerformedActionsCount("_id2", 1, false);
        refresh();

        SearchResponse response = client().prepareSearch("output1").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("key4").toString(), equalTo("30"));

        response = client().prepareSearch("output2").get();
        assertNoFailures(response);
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("key4").toString(), equalTo("30"));
    }

    private void executeWatch(String watchId) {
        new ExecuteWatchRequestBuilder(client(), watchId).setRecordExecution(true).get();
    }
}
