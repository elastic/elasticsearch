/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScriptLanguagesInfoTests extends ESTestCase {
    public void testEmptyTypesAllowedReturnsAllTypes() {
        ScriptService ss = getMockScriptService(Settings.EMPTY);
        ScriptLanguagesInfo info = ss.getScriptLanguages();
        ScriptType[] types = ScriptType.values();
        assertEquals(types.length, info.typesAllowed.size());
        for (ScriptType type : types) {
            assertTrue("[" + type.getName() + "] is allowed", info.typesAllowed.contains(type.getName()));
        }
    }

    public void testSingleTypesAllowedReturnsThatType() {
        for (ScriptType type : ScriptType.values()) {
            ScriptService ss = getMockScriptService(Settings.builder().put("script.allowed_types", type.getName()).build());
            ScriptLanguagesInfo info = ss.getScriptLanguages();
            assertEquals(1, info.typesAllowed.size());
            assertTrue("[" + type.getName() + "] is allowed", info.typesAllowed.contains(type.getName()));
        }
    }

    public void testBothTypesAllowedReturnsBothTypes() {
        List<String> types = Arrays.stream(ScriptType.values()).map(ScriptType::getName).toList();
        Settings.Builder settings = Settings.builder().putList("script.allowed_types", types);
        ScriptService ss = getMockScriptService(settings.build());
        ScriptLanguagesInfo info = ss.getScriptLanguages();
        assertEquals(types.size(), info.typesAllowed.size());
        for (String type : types) {
            assertTrue("[" + type + "] is allowed", info.typesAllowed.contains(type));
        }
    }

    private ScriptService getMockScriptService(Settings settings) {
        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Collections.singletonMap("test_script", script -> 1),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(settings, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    public interface MiscContext {
        void execute();

        Object newInstance();
    }

    public void testOnlyScriptEngineContextsReturned() {
        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Collections.singletonMap("test_script", script -> 1),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        Map<String, ScriptContext<?>> mockContexts = scriptEngine.getSupportedContexts()
            .stream()
            .collect(Collectors.toMap(c -> c.name, Function.identity()));
        String miscContext = "misc_context";
        assertFalse(mockContexts.containsKey(miscContext));

        Map<String, ScriptContext<?>> mockAndMiscContexts = new HashMap<>(mockContexts);
        mockAndMiscContexts.put(miscContext, new ScriptContext<>(miscContext, MiscContext.class));

        ScriptService ss = new ScriptService(Settings.EMPTY, engines, mockAndMiscContexts, () -> 1L);
        ScriptLanguagesInfo info = ss.getScriptLanguages();

        assertTrue(info.languageContexts.containsKey(MockScriptEngine.NAME));
        assertEquals(1, info.languageContexts.size());
        assertEquals(mockContexts.keySet(), info.languageContexts.get(MockScriptEngine.NAME));
    }

    public void testContextsAllowedSettingRespected() {
        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Collections.singletonMap("test_script", script -> 1),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        Map<String, ScriptContext<?>> mockContexts = scriptEngine.getSupportedContexts()
            .stream()
            .collect(Collectors.toMap(c -> c.name, Function.identity()));

        List<String> allContexts = new ArrayList<>(mockContexts.keySet());
        List<String> allowed = allContexts.subList(0, allContexts.size() / 2);
        String miscContext = "misc_context";
        allowed.add(miscContext);
        // check that allowing more than available doesn't pollute the returned contexts
        Settings.Builder settings = Settings.builder().putList("script.allowed_contexts", allowed);

        Map<String, ScriptContext<?>> mockAndMiscContexts = new HashMap<>(mockContexts);
        mockAndMiscContexts.put(miscContext, new ScriptContext<>(miscContext, MiscContext.class));

        ScriptService ss = new ScriptService(settings.build(), engines, mockAndMiscContexts, () -> 1L);
        ScriptLanguagesInfo info = ss.getScriptLanguages();

        assertTrue(info.languageContexts.containsKey(MockScriptEngine.NAME));
        assertEquals(1, info.languageContexts.size());
        assertEquals(new HashSet<>(allContexts.subList(0, allContexts.size() / 2)), info.languageContexts.get(MockScriptEngine.NAME));
    }
}
