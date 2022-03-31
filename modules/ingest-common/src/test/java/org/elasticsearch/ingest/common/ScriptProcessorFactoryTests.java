/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorFactoryTests extends ESTestCase {

    private ScriptProcessor.Factory factory;
    private static final Map<String, String> INGEST_SCRIPT_PARAM_TO_TYPE = Map.of("id", "stored", "source", "inline");

    @Before
    public void init() {
        factory = new ScriptProcessor.Factory(mock(ScriptService.class));
    }

    public void testFactoryValidationWithDefaultLang() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        configMap.put(randomType, "foo");
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(INGEST_SCRIPT_PARAM_TO_TYPE.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
    }

    public void testFactoryValidationWithParams() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        Map<String, Object> randomParams = Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10));
        configMap.put(randomType, "foo");
        configMap.put("params", randomParams);
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(INGEST_SCRIPT_PARAM_TO_TYPE.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(randomParams));
    }

    public void testFactoryValidationForMultipleScriptingTypes() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("id", "foo");
        configMap.put("source", "bar");
        configMap.put("lang", "mockscript");

        XContentParseException exception = expectThrows(
            XContentParseException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap)
        );
        assertThat(exception.getMessage(), containsString("[script] failed to parse field [source]"));
    }

    public void testFactoryValidationAtLeastOneScriptingType() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("lang", "mockscript");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap)
        );

        assertThat(exception.getMessage(), is("must specify either [source] for an inline script or [id] for a stored script"));
    }

    public void testInlineBackcompat() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("inline", "code");

        factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertWarnings("Deprecated field [inline] used, expected [source] instead");
    }

    public void testFactoryInvalidateWithInvalidCompiledScript() throws Exception {
        String randomType = randomFrom("source", "id");
        ScriptService mockedScriptService = mock(ScriptService.class);
        ScriptException thrownException = new ScriptException(
            "compile-time exception",
            new RuntimeException(),
            Collections.emptyList(),
            "script",
            "mockscript"
        );
        when(mockedScriptService.compile(any(), any())).thenThrow(thrownException);
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(randomType, "my_script");

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap)
        );

        assertThat(exception.getMessage(), is("compile-time exception"));
    }

    public void testInlineIsCompiled() throws Exception {
        String scriptName = "foo";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    ctx.put("foo", "bar");
                    return null;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );
        factory = new ScriptProcessor.Factory(scriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source", scriptName);
        ScriptProcessor processor = factory.create(null, null, randomAlphaOfLength(10), configMap);
        assertThat(processor.getScript().getLang(), equalTo(Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType(), equalTo(ScriptType.INLINE));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
        assertNotNull(processor.getPrecompiledIngestScript());
        Map<String, Object> ctx = new HashMap<>();
        processor.getPrecompiledIngestScript().execute(ctx);
        assertThat(ctx.get("foo"), equalTo("bar"));
    }

    public void testStoredIsNotCompiled() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("id", "script_name");
        ScriptProcessor processor = factory.create(null, null, randomAlphaOfLength(10), configMap);
        assertNull(processor.getScript().getLang());
        assertThat(processor.getScript().getType(), equalTo(ScriptType.STORED));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
        assertNull(processor.getPrecompiledIngestScript());
    }
}
