/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.script;


import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getScriptServiceProxy;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/724")
public class ScriptConditionTests extends ESTestCase {
    ThreadPool tp = null;

    @Before
    public void init() {
        tp = new ThreadPool(ThreadPool.Names.SAME);
    }

    @After
    public void cleanup() {
        tp.shutdownNow();
    }

    public void testExecute() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(Script.inline("ctx.payload.hits.total > 1").build()), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());
    }

    public void testExecuteMergedParams() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        Script script = Script.inline("ctx.payload.hits.total > threshold")
                .lang(Script.DEFAULT_LANG).params(singletonMap("threshold", 1)).build();
        ExecutableScriptCondition executable = new ExecutableScriptCondition(
                new ScriptCondition(script), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(executable.execute(ctx).met());
    }

    public void testParserValid() throws Exception {
        ScriptConditionFactory factory = new ScriptConditionFactory(Settings.builder().build(), getScriptServiceProxy(tp));

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
        ScriptConditionFactory factory = new ScriptConditionFactory(Settings.builder().build(), getScriptServiceProxy(tp));
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
        ScriptConditionFactory conditionParser = new ScriptConditionFactory(Settings.builder().build(), getScriptServiceProxy(tp));
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
        try {
            conditionParser.createExecutable(scriptCondition);
            fail("expected a condition validation exception trying to create an executable with a bad or missing script");
        } catch (ScriptException e) {
            // TODO add these when the test if fixed
            // assertThat(e.getMessage(), is("ASDF"));
        }
    }

    public void testScriptConditionParser_badLang() throws Exception {
        ScriptConditionFactory conditionParser = new ScriptConditionFactory(Settings.builder().build(), getScriptServiceProxy(tp));
        ScriptType scriptType = ScriptType.INLINE;
        String script = "return true";
        XContentBuilder builder = createConditionContent(script, "not_a_valid_lang", scriptType);
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptCondition scriptCondition = conditionParser.parseCondition("_watch", parser);
        try {
            conditionParser.createExecutable(scriptCondition);
            fail("expected a condition validation exception trying to create an executable with an invalid language");
        } catch (ScriptException e) {
            // TODO add these when the test if fixed
            // assertThat(e.getMessage(), is("ASDF"));
        }
    }

    public void testScriptConditionThrowException() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        ExecutableScriptCondition condition =
                new ExecutableScriptCondition(new ScriptCondition(Script.inline("assert false").build()), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        ScriptCondition.Result result = condition.execute(ctx);
        assertThat(result, notNullValue());
        assertThat(result.status(), is(Condition.Result.Status.FAILURE));
        assertThat(result.reason(), notNullValue());
        assertThat(result.reason(), containsString("Assertion"));
    }

    public void testScriptConditionReturnObject() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        ExecutableScriptCondition condition =
                new ExecutableScriptCondition(new ScriptCondition(Script.inline("return new Object()").build()), logger, scriptService);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500L, new ShardSearchFailure[0]);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        ScriptCondition.Result result = condition.execute(ctx);
        assertThat(result, notNullValue());
        assertThat(result.status(), is(Condition.Result.Status.FAILURE));
        assertThat(result.reason(), notNullValue());
        assertThat(result.reason(), containsString("ScriptException"));
    }

    public void testScriptConditionAccessCtx() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        ExecutableScriptCondition condition = new ExecutableScriptCondition(
                new ScriptCondition(Script.inline("ctx.trigger.scheduled_time.getMillis() < System.currentTimeMillis() ").build()),
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
