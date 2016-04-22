/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.watcher.test.WatcherTestUtils.EMPTY_PAYLOAD;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getScriptServiceProxy;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.elasticsearch.watcher.test.WatcherTestUtils.simplePayload;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ScriptTransformTests extends ESTestCase {
    ThreadPool tp = null;

    @Before
    public void init() {
        tp = new ThreadPool(ThreadPool.Names.SAME);
    }

    @After
    public void cleanup() {
        tp.shutdownNow();
    }

    public void testExecute_MapValue() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = scriptBuilder(type, "_script").lang("_lang").params(params).build();
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script)).thenReturn(compiledScript);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Payload payload = simplePayload("key", "value");

        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        Map<String, Object> transformed = singletonMap("key", "value");

        ExecutableScript executable = mock(ExecutableScript.class);
        when(executable.run()).thenReturn(transformed);
        when(service.executable(compiledScript, model)).thenReturn(executable);

        Transform.Result result = transform.execute(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(ScriptTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertThat(result.payload().data(), equalTo(transformed));
    }

    public void testExecuteMapValueFailure() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = scriptBuilder(type, "_script").lang("_lang").params(params).build();
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script)).thenReturn(compiledScript);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Payload payload = simplePayload("key", "value");

        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        ExecutableScript executable = mock(ExecutableScript.class);
        when(executable.run()).thenThrow(new RuntimeException("_error"));
        when(service.executable(compiledScript, model)).thenReturn(executable);

        Transform.Result result = transform.execute(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(ScriptTransform.TYPE));
        assertThat(result.status(), is(Transform.Result.Status.FAILURE));
        assertThat(result.reason(), containsString("_error"));
    }

    public void testExecuteNonMapValue() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);

        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = scriptBuilder(type, "_script").lang("_lang").params(params).build();
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script)).thenReturn(compiledScript);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Payload payload = simplePayload("key", "value");

        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        ExecutableScript executable = mock(ExecutableScript.class);
        Object value = randomFrom("value", 1, new String[] { "value" }, Arrays.asList("value"), singleton("value"));
        when(executable.run()).thenReturn(value);
        when(service.executable(compiledScript, model)).thenReturn(executable);

        Transform.Result result = transform.execute(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(ScriptTransform.TYPE));
        assertThat(result.payload().data().size(), is(1));
        assertThat(result.payload().data(), hasEntry("_value", value));
    }

    public void testParser() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        ScriptType type = randomFrom(ScriptType.values());
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field(scriptTypeField(type), "_script");
        builder.field("lang", "_lang");
        builder.startObject("params").field("key", "value").endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ExecutableScriptTransform transform = new ScriptTransformFactory(Settings.EMPTY, service).parseExecutable("_id", parser);
        Script script = scriptBuilder(type, "_script").lang("_lang").params(singletonMap("key", "value")).build();
        assertThat(transform.transform().getScript(), equalTo(script));
    }

    public void testParserString() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        XContentBuilder builder = jsonBuilder().value("_script");

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ExecutableScriptTransform transform = new ScriptTransformFactory(Settings.EMPTY, service).parseExecutable("_id", parser);
        assertThat(transform.transform().getScript(), equalTo(Script.defaultType("_script").build()));
    }

    public void testScriptConditionParserBadScript() throws Exception {
        ScriptTransformFactory transformFactory = new ScriptTransformFactory(Settings.builder().build(), getScriptServiceProxy(tp));
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

        XContentBuilder builder = jsonBuilder().startObject()
                .field(scriptTypeField(scriptType), script)
                .field("lang", "groovy")
                .startObject("params").field("key", "value").endObject()
                .endObject();

        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptTransform scriptTransform = transformFactory.parseTransform("_watch", parser);
        try {
            transformFactory.createExecutable(scriptTransform);
            fail("expected a transform validation exception trying to create an executable with a bad or missing script");
        } catch (ScriptException e) {
            // I don't think this is what this test intended to check!
            assertThat(e.getMessage(), containsString("script_lang not supported [groovy]"));
        }
    }

    public void testScriptConditionParserBadLang() throws Exception {
        ScriptTransformFactory transformFactory = new ScriptTransformFactory(Settings.builder().build(), getScriptServiceProxy(tp));
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script = "return true";
        XContentBuilder builder = jsonBuilder().startObject()
                .field(scriptTypeField(scriptType), script)
                .field("lang", "not_a_valid_lang")
                .startObject("params").field("key", "value").endObject()
                .endObject();


        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ScriptTransform scriptCondition = transformFactory.parseTransform("_watch", parser);
        try {
            transformFactory.createExecutable(scriptCondition);
            fail("expected a transform validation exception trying to create an executable with an invalid language");
        } catch (ScriptException e) {
            assertThat(e.getMessage(), containsString("script_lang not supported [not_a_valid_lang]"));
        }
    }

    static Script.Builder scriptBuilder(ScriptType type, String script) {
        switch (type) {
            case INLINE:    return Script.inline(script);
            case FILE:      return Script.file(script);
            case STORED:   return Script.indexed(script);
            default:
                throw illegalArgument("unsupported script type [{}]", type);
        }
    }

    static String scriptTypeField(ScriptType type) {
        switch (type) {
            case INLINE: return "inline";
            case FILE: return "file";
            case STORED: return "id";
            default:
                throw illegalArgument("unsupported script type [{}]", type);
        }
    }
}
