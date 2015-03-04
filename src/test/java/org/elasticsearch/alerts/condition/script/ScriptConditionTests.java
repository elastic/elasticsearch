/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.condition.ConditionException;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.alerts.test.AlertsTestUtils.mockExecutionContext;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class ScriptConditionTests extends ElasticsearchTestCase {

    ThreadPool tp = null;

    @Before
    public void init() {
        tp = new ThreadPool(ThreadPool.Names.SAME);
    }

    @After
    public void cleanup() {
        tp.shutdownNow();
    }


    @Test
    public void testExecute() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        ScriptCondition condition = new ScriptCondition(logger, scriptService, new Script("ctx.payload.hits.total > 1"));
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500l, new ShardSearchFailure[0]);
        ExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());
    }

    @Test
    public void testExecute_MergedParams() throws Exception {
        ScriptServiceProxy scriptService = getScriptServiceProxy(tp);
        Script script = new Script("ctx.payload.hits.total > threshold", ScriptService.ScriptType.INLINE, ScriptService.DEFAULT_LANG, ImmutableMap.<String, Object>of("threshold", 1));
        ScriptCondition condition = new ScriptCondition(logger, scriptService, script);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500l, new ShardSearchFailure[0]);
        ExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());
    }

    @Test
    public void testParser_Valid() throws Exception {
        ScriptCondition.Parser conditionParser = new ScriptCondition.Parser(ImmutableSettings.settingsBuilder().build(), getScriptServiceProxy(tp));

        XContentBuilder builder = createConditionContent("ctx.payload.hits.total > 1", null, null);
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptCondition condition = conditionParser.parse(parser);

        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500l, new ShardSearchFailure[0]);
        ExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertFalse(condition.execute(ctx).met());


        builder = createConditionContent("return true", null, null);
        parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        condition = conditionParser.parse(parser);

        ctx = mockExecutionContext("_name", new Payload.XContent(response));

        assertTrue(condition.execute(ctx).met());
    }

    @Test(expected = AlertsSettingsException.class)
    public void testParser_InValid() throws Exception {
        ScriptCondition.Parser conditionParser = new ScriptCondition.Parser(ImmutableSettings.settingsBuilder().build(), getScriptServiceProxy(tp));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        conditionParser.parse(parser);
        fail("expected a condition exception trying to parse an invalid condition XContent");
    }


    @Test
    public void testScriptResultParser_Valid() throws Exception {
        ScriptCondition.Parser conditionParser = new ScriptCondition.Parser(ImmutableSettings.settingsBuilder().build(), getScriptServiceProxy(tp));

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("met", true );
        builder.endObject();

        ScriptCondition.Result scriptResult = conditionParser.parseResult(XContentFactory.xContent(builder.bytes()).createParser(builder.bytes()));
        assertTrue(scriptResult.met());

        builder = jsonBuilder();
        builder.startObject();
        builder.field("met", false );
        builder.endObject();

        scriptResult = conditionParser.parseResult(XContentFactory.xContent(builder.bytes()).createParser(builder.bytes()));
        assertFalse(scriptResult.met());
    }

    @Test(expected = ConditionException.class)
    public void testScriptResultParser_Invalid() throws Exception {
        ScriptCondition.Parser conditionParser = new ScriptCondition.Parser(ImmutableSettings.settingsBuilder().build(), getScriptServiceProxy(tp));

        XContentBuilder builder = jsonBuilder();
        builder.startObject().endObject();

        try {
            conditionParser.parseResult(XContentFactory.xContent(builder.bytes()).createParser(builder.bytes()));
        } catch (Throwable t) {
            throw t;
        }
        fail("expected a condition exception trying to parse an invalid condition XContent");
    }

    private static ScriptServiceProxy getScriptServiceProxy(ThreadPool tp) {
        Settings settings = ImmutableSettings.settingsBuilder().build();
        GroovyScriptEngineService groovyScriptEngineService = new GroovyScriptEngineService(settings);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(groovyScriptEngineService);
        return ScriptServiceProxy.of(new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp)));
    }

    private static XContentBuilder createConditionContent(String script, String scriptLang, ScriptService.ScriptType scriptType) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("script", script);
        if (scriptLang != null) {
            builder.field("lang", scriptLang);
        }
        if (scriptType != null) {
            builder.field("type", scriptType.toString());
        }
        return builder.endObject();
    }


}
