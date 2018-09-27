/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;


import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ScriptConditionTests extends ESTestCase {

    private ScriptService scriptService;

    @Before
    public void init() throws IOException {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put("return true", s -> true);
        scripts.put("return new Object()", s -> new Object());

        scripts.put("ctx.trigger.scheduled_time.getMillis() < new Date().time", vars -> {
            DateTime scheduledTime = (DateTime) XContentMapValues.extractValue("ctx.trigger.scheduled_time", vars);
            return scheduledTime.getMillis() < new Date().getTime();
        });

        scripts.put("null.foo", s -> {
            throw new ScriptException("Error evaluating null.foo", new IllegalArgumentException(), emptyList(),
                    "null.foo", AbstractWatcherIntegrationTestCase.WATCHER_LANG);
        });

        scripts.put("ctx.payload.hits.total > 1", vars -> {
            int total = (int) XContentMapValues.extractValue("ctx.payload.hits.total", vars);
            return total > 1;
        });

        scripts.put("ctx.payload.hits.total > threshold", vars -> {
            int total = (int) XContentMapValues.extractValue("ctx.payload.hits.total", vars);
            int threshold = (int) XContentMapValues.extractValue("threshold", vars);
            return total > threshold;
        });

        ScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        scriptService = new ScriptService(Settings.EMPTY, Collections.singletonMap(engine.getType(), engine),
            Collections.singletonMap(Watcher.SCRIPT_EXECUTABLE_CONTEXT.name, Watcher.SCRIPT_EXECUTABLE_CONTEXT));

        ClusterState.Builder clusterState = new ClusterState.Builder(new ClusterName("_name"));
        clusterState.metaData(MetaData.builder().putCustom(ScriptMetaData.TYPE, new ScriptMetaData.Builder(null).build()));
        ClusterState cs = clusterState.build();
        scriptService.applyClusterState(new ClusterChangedEvent("_source", cs, cs));
    }

    public void testExecute() throws Exception {
        Script script = mockScript("ctx.payload.hits.total > 1");
        ScriptCondition condition = new ScriptCondition(script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());
    }

    public void testExecuteMergedParams() throws Exception {
        Script script = new Script(ScriptType.INLINE, "mockscript", "ctx.payload.hits.total > threshold", singletonMap("threshold", 1));
        ScriptCondition executable = new ScriptCondition(script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(executable.execute(ctx).met());
    }

    public void testParserValid() throws Exception {

        XContentBuilder builder = createConditionContent("ctx.payload.hits.total > 1", "mockscript", ScriptType.INLINE);

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableCondition executable = ScriptCondition.parse(scriptService, "_watch", parser);

        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertFalse(executable.execute(ctx).met());


        builder = createConditionContent("return true", "mockscript", ScriptType.INLINE);
        parser = createParser(builder);
        parser.nextToken();
        executable = ScriptCondition.parse(scriptService, "_watch", parser);

        ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertTrue(executable.execute(ctx).met());
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            ScriptCondition.parse(scriptService, "_id", parser);
            fail("expected a condition exception trying to parse an invalid condition XContent");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    containsString("must specify either [source] for an inline script or [id] for a stored script"));
        }
    }

    public void testScriptConditionParserBadScript() throws Exception {
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script;
        Class<? extends Exception> expectedException;
        switch (scriptType) {
            case STORED:
                expectedException = ResourceNotFoundException.class;
                script = "nonExisting_script";
                break;
            default:
                expectedException = GeneralScriptException.class;
                script = "foo = = 1";
        }
        XContentBuilder builder = createConditionContent(script, "mockscript", scriptType);
        XContentParser parser = createParser(builder);
        parser.nextToken();

        expectThrows(expectedException,
                () -> ScriptCondition.parse(scriptService, "_watch", parser));
    }

    public void testScriptConditionParser_badLang() throws Exception {
        String script = "return true";
        XContentBuilder builder = createConditionContent(script, "not_a_valid_lang", ScriptType.INLINE);
        XContentParser parser = createParser(builder);
        parser.nextToken();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> ScriptCondition.parse(scriptService, "_watch", parser));
        assertThat(exception.getMessage(), containsString("script_lang not supported [not_a_valid_lang]"));
    }

    public void testScriptConditionThrowException() throws Exception {
        Script script = mockScript("null.foo");
        ScriptCondition condition = new ScriptCondition(
                script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        ScriptException exception = expectThrows(ScriptException.class, () -> condition.execute(ctx));
        assertThat(exception.getMessage(), containsString("Error evaluating null.foo"));
    }

    public void testScriptConditionReturnObjectThrowsException() throws Exception {
        Script script = mockScript("return new Object()");
        ScriptCondition condition = new ScriptCondition(script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        Exception exception = expectThrows(IllegalStateException.class, () -> condition.execute(ctx));
        assertThat(exception.getMessage(),
                containsString("condition [script] must return a boolean value (true|false) but instead returned [_name]"));
    }

    public void testScriptConditionAccessCtx() throws Exception {
        Script script = mockScript("ctx.trigger.scheduled_time.getMillis() < new Date().time");
        ScriptCondition condition = new ScriptCondition(script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        WatchExecutionContext ctx = mockExecutionContext("_name", new DateTime(DateTimeZone.UTC), new Payload.XContent(response));
        Thread.sleep(10);
        assertThat(condition.execute(ctx).met(), is(true));
    }

    private static XContentBuilder createConditionContent(String script, String scriptLang, ScriptType scriptType) throws IOException {
        XContentBuilder builder = jsonBuilder();
        if (scriptType == null) {
            return builder.value(script);
        }
        builder.startObject();
        switch (scriptType) {
            case INLINE:
                builder.field("source", script);
                break;
            case STORED:
                builder.field("id", script);
                break;
            default:
                throw illegalArgument("unsupported script type [{}]", scriptType);
        }
        if (scriptLang != null && scriptType != ScriptType.STORED) {
            builder.field("lang", scriptLang);
        }
        return builder.endObject();
    }
}
