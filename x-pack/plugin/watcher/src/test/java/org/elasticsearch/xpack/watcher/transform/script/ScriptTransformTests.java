/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.Watcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
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

    public void testExecuteMapValue() throws Exception {
        ScriptService service = mock(ScriptService.class);
        ScriptType type = randomFrom(ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = new Script(type, type == ScriptType.STORED ? null : "_lang", "_script", params);
        WatcherTransformScript.Factory factory = mock(WatcherTransformScript.Factory.class);
        when(service.compile(script, WatcherTransformScript.CONTEXT)).thenReturn(factory);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);

        Payload payload = new Payload.Simple("key", "value");

        Map<String, Object> transformed = singletonMap("key", "value");

        WatcherTransformScript executable = mock(WatcherTransformScript.class);
        when(executable.execute()).thenReturn(transformed);
        when(factory.newInstance(params, ctx, payload)).thenReturn(executable);

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
        Script script = new Script(type, type == ScriptType.STORED ? null : "_lang", "_script", params);
        WatcherTransformScript.Factory factory = mock(WatcherTransformScript.Factory.class);
        when(service.compile(script, WatcherTransformScript.CONTEXT)).thenReturn(factory);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);

        Payload payload = new Payload.Simple("key", "value");

        WatcherTransformScript executable = mock(WatcherTransformScript.class);
        when(executable.execute()).thenThrow(new RuntimeException("_error"));
        when(factory.newInstance(params, ctx, payload)).thenReturn(executable);

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
        Script script = new Script(type, type == ScriptType.STORED ? null : "_lang", "_script", params);
        WatcherTransformScript.Factory factory = mock(WatcherTransformScript.Factory.class);
        when(service.compile(script, WatcherTransformScript.CONTEXT)).thenReturn(factory);
        ExecutableScriptTransform transform = new ExecutableScriptTransform(new ScriptTransform(script), logger, service);

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);

        Payload payload = new Payload.Simple("key", "value");

        WatcherTransformScript executable = mock(WatcherTransformScript.class);
        Object value = randomFrom("value", 1, new String[] { "value" }, Collections.singletonList("value"), singleton("value"));
        when(executable.execute()).thenReturn(value);
        when(factory.newInstance(params, ctx, payload)).thenReturn(executable);

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
        ExecutableScriptTransform transform = new ScriptTransformFactory(service).parseExecutable("_id", parser);
        Script script = new Script(type, type == ScriptType.STORED ? null : "_lang", "_script", singletonMap("key", "value"));
        assertThat(transform.transform().getScript(), equalTo(script));
    }

    public void testParserString() throws Exception {
        ScriptService service = mock(ScriptService.class);
        XContentBuilder builder = jsonBuilder().value("_script");

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableScriptTransform transform = new ScriptTransformFactory(service).parseExecutable("_id", parser);
        assertEquals(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "_script", emptyMap()), transform.transform().getScript());
    }

    public void testScriptConditionParserBadScript() throws Exception {
        ScriptService scriptService = mock(ScriptService.class);
        String errorMessage = "expected error message";
        ScriptException scriptException = new ScriptException(errorMessage, new RuntimeException("foo"),
                Collections.emptyList(), "whatever", "whatever");
        when(scriptService.compile(anyObject(), eq(WatcherTransformScript.CONTEXT))).thenThrow(scriptException);

        ScriptTransformFactory transformFactory = new ScriptTransformFactory(scriptService);

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
        ScriptTransformFactory transformFactory = new ScriptTransformFactory(createScriptService());
        String script = "return true";
        XContentBuilder builder = jsonBuilder().startObject()
                .field(scriptTypeField(ScriptType.INLINE), script)
                .field("lang", "not_a_valid_lang")
                .startObject("params").field("key", "value").endObject()
                .endObject();


        XContentParser parser = createParser(builder);
        parser.nextToken();
        ScriptTransform scriptCondition = transformFactory.parseTransform("_watch", parser);
        Exception e = expectThrows(IllegalArgumentException.class, () -> transformFactory.createExecutable(scriptCondition));
        assertThat(e.getMessage(), containsString("script_lang not supported [not_a_valid_lang]"));
    }

    static String scriptTypeField(ScriptType type) {
        switch (type) {
            case INLINE: return "source";
            case STORED: return "id";
            default:
                throw illegalArgument("unsupported script type [{}]", type);
        }
    }

    public static ScriptService createScriptService() throws Exception {
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .build();
        Map<String, ScriptContext<?>> contexts = new HashMap<>(ScriptModule.CORE_CONTEXTS);
        contexts.put(WatcherTransformScript.CONTEXT.name, WatcherTransformScript.CONTEXT);
        contexts.put(Watcher.SCRIPT_TEMPLATE_CONTEXT.name, Watcher.SCRIPT_TEMPLATE_CONTEXT);
        return new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap());
    }
}
