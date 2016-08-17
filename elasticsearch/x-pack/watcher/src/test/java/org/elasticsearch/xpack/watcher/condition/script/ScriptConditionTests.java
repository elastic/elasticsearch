/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition.script;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.script.ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING;
import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalArgument;
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

        ScriptEngineService engine = new MockScriptEngine(AbstractWatcherIntegrationTestCase.WATCHER_LANG, scripts);

        ScriptEngineRegistry registry = new ScriptEngineRegistry(singleton(engine));
        ScriptContextRegistry contextRegistry = new ScriptContextRegistry(singleton(new ScriptContext.Plugin("xpack", "watch")));
        ScriptSettings scriptSettings = new ScriptSettings(registry, contextRegistry);

        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .build();

        scriptService = new ScriptService(settings, new Environment(settings), null, registry, contextRegistry, scriptSettings);
    }

    public void testExecute() throws Exception {
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(new Script("ctx.payload.hits.total > 1")), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());
    }

    public void testExecuteMergedParams() throws Exception {
        Script script = new Script("ctx.payload.hits.total > threshold", ScriptType.INLINE, null, singletonMap("threshold", 1));
        ExecutableScriptCondition executable = new ExecutableScriptCondition(new ScriptCondition(script), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(executable.execute(ctx).met());
    }

    public void testParserValid() throws Exception {
        ScriptConditionFactory factory = new ScriptConditionFactory(Settings.builder().build(), scriptService);

        XContentBuilder builder = createConditionContent("ctx.payload.hits.total > 1", null, ScriptType.INLINE);

        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptCondition condition = factory.parseCondition("_watch", parser);
        ExecutableScriptCondition executable = factory.createExecutable(condition);

        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertFalse(executable.execute(ctx).met());


        builder = createConditionContent("return true", null, ScriptType.INLINE);
        parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        condition = factory.parseCondition("_watch", parser);
        executable = factory.createExecutable(condition);

        ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertTrue(executable.execute(ctx).met());
    }

    public void testParserInvalid() throws Exception {
        ScriptConditionFactory factory = new ScriptConditionFactory(Settings.builder().build(), scriptService);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        try {
            factory.parseCondition("_id", parser);
            fail("expected a condition exception trying to parse an invalid condition XContent");
        } catch (ElasticsearchParseException e) {
            // TODO add these when the test if fixed
            // assertThat(e.getMessage(), is("ASDF"));
        }
    }

    public void testScriptConditionParserBadScript() throws Exception {
        ScriptConditionFactory conditionParser = new ScriptConditionFactory(Settings.builder().build(), scriptService);
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script;
        switch (scriptType) {
            case STORED:
            case FILE:
                script = "nonExisting_script";
                break;
            case INLINE:
            default:
                script = "foo = = 1";
        }
        XContentBuilder builder = createConditionContent(script, "groovy", scriptType);
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptCondition scriptCondition = conditionParser.parseCondition("_watch", parser);
        expectThrows(GeneralScriptException.class,
                () -> conditionParser.createExecutable(scriptCondition));
    }

    public void testScriptConditionParser_badLang() throws Exception {
        ScriptConditionFactory conditionParser = new ScriptConditionFactory(Settings.builder().build(), scriptService);
        String script = "return true";
        XContentBuilder builder = createConditionContent(script, "not_a_valid_lang", ScriptType.INLINE);
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptCondition scriptCondition = conditionParser.parseCondition("_watch", parser);
        GeneralScriptException exception = expectThrows(GeneralScriptException.class,
                () -> conditionParser.createExecutable(scriptCondition));
        assertThat(exception.getMessage(), containsString("script_lang not supported [not_a_valid_lang]]"));
    }

    public void testScriptConditionThrowException() throws Exception {
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(new Script("null.foo")), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        ScriptException exception = expectThrows(ScriptException.class, () -> condition.execute(ctx));
        assertThat(exception.getMessage(), containsString("Error evaluating null.foo"));
    }

    public void testScriptConditionReturnObjectThrowsException() throws Exception {
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(new Script("return new Object()")), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        Exception exception = expectThrows(GeneralScriptException.class, () -> condition.execute(ctx));
        assertThat(exception.getMessage(),
                containsString("condition [script] must return a boolean value (true|false) but instead returned [_name]"));
    }

    public void testScriptConditionAccessCtx() throws Exception {
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(new Script("ctx.trigger.scheduled_time.getMillis() < new Date().time")),
                logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
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
                builder.field("inline", script);
                break;
            case FILE:
                builder.field("file", script);
                break;
            case STORED:
                builder.field("id", script);
                break;
            default:
                throw illegalArgument("unsupported script type [{}]", scriptType);
        }
        if (scriptLang != null) {
            builder.field("lang", scriptLang);
        }
        return builder.endObject();
    }
}
