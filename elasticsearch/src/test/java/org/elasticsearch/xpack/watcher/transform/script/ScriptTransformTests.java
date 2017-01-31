/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.junit.After;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.EMPTY_PAYLOAD;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.createScriptService;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.simplePayload;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptTransformTests extends ESTestCase {

    private final ThreadPool tp = new TestThreadPool(ThreadPool.Names.SAME);

    @After
    public void cleanup() {
        tp.shutdownNow();
    }

    public void testExecuteMapValue() throws Exception {
        ScriptService service = mock(ScriptService.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = new Script(type, "_lang", "_script", params);
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script, Watcher.SCRIPT_CONTEXT)).thenReturn(compiledScript);
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
        ScriptService service = mock(ScriptService.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = new Script(type, "_lang", "_script", params);
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script, Watcher.SCRIPT_CONTEXT)).thenReturn(compiledScript);
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
        ScriptService service = mock(ScriptService.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = new Script(type, "_lang", "_script", params);
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(service.compile(script, Watcher.SCRIPT_CONTEXT)).thenReturn(compiledScript);
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
        ScriptService service = mock(ScriptService.class);
        ScriptType type = randomFrom(ScriptType.values());
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field(scriptTypeField(type), "_script");
        if (type != ScriptType.STORED) {
            builder.field("lang", "_lang");
        }
        builder.startObject("params").field("key", "value").endObject();
        builder.endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableScriptTransform transform = new ScriptTransformFactory(Settings.EMPTY, service).parseExecutable("_id", parser);
        Script script = new Script(type, type == ScriptType.STORED ? null : "_lang", "_script", singletonMap("key", "value"));
        assertThat(transform.transform().getScript(), equalTo(script));
    }

    public void testParserString() throws Exception {
        ScriptService service = mock(ScriptService.class);
        XContentBuilder builder = jsonBuilder().value("_script");

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableScriptTransform transform = new ScriptTransformFactory(Settings.EMPTY, service).parseExecutable("_id", parser);
        assertThat(transform.transform().getScript(), equalTo(new Script("_script")));
    }

    public void testScriptConditionParserBadScript() throws Exception {
        ScriptService scriptService = mock(ScriptService.class);
        String errorMessage = "expected error message";
        ScriptException scriptException = new ScriptException(errorMessage, new RuntimeException("foo"),
                Collections.emptyList(), "whatever", "whatever");
        when(scriptService.compile(anyObject(), eq(Watcher.SCRIPT_CONTEXT))).thenThrow(scriptException);

        ScriptTransformFactory transformFactory = new ScriptTransformFactory(Settings.builder().build(), scriptService);

        XContentBuilder builder = jsonBuilder().startObject()
                .field(scriptTypeField(randomFrom(ScriptType.values())), "whatever")
                .startObject("params").field("key", "value").endObject()
                .endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ScriptTransform scriptTransform = transformFactory.parseTransform("_watch", parser);
        Exception e = expectThrows(ScriptException.class, () -> transformFactory.createExecutable(scriptTransform));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    public void testScriptConditionParserBadLang() throws Exception {
        ScriptTransformFactory transformFactory = new ScriptTransformFactory(Settings.builder().build(), createScriptService(tp));
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script = "return true";
        XContentBuilder builder = jsonBuilder().startObject()
                .field(scriptTypeField(scriptType), script)
                .field("lang", "not_a_valid_lang")
                .startObject("params").field("key", "value").endObject()
                .endObject();


        XContentParser parser = createParser(builder);
        parser.nextToken();
        ScriptTransform scriptCondition = transformFactory.parseTransform("_watch", parser);
        Exception e = expectThrows(IllegalArgumentException.class, () -> transformFactory.createExecutable(scriptCondition));
        if (scriptType == ScriptType.STORED) {
            assertThat(e.getMessage(), containsString("unable to get stored script with unsupported lang [not_a_valid_lang]"));
            assertWarnings("specifying the field [lang] for executing stored scripts is deprecated;" +
                    " use only the field [stored] to specify an <id>");
        } else {
            assertThat(e.getMessage(), containsString("script_lang not supported [not_a_valid_lang]"));
        }
    }

    static String scriptTypeField(ScriptType type) {
        switch (type) {
            case INLINE: return "inline";
            case FILE: return "file";
            case STORED: return "stored";
            default:
                throw illegalArgument("unsupported script type [{}]", type);
        }
    }
}
