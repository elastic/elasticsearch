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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class DoubleValuesScriptTests extends ESTestCase {
    private ScriptService buildScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        for (int i = 0; i < 20; ++i) {
            scripts.put(i + "+" + i, p -> null); // only care about compilation, not execution
        }
        var scriptEngine = new MockScriptEngine("test", scripts, Collections.emptyMap());
        return new ScriptService(Settings.EMPTY, Map.of("test", scriptEngine), new HashMap<>(ScriptModule.CORE_CONTEXTS), () -> 1L);
    }

    public void testDoubleValuesScriptContextCanBeCompiled() throws IOException {
        var scriptService = buildScriptService();
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        var result = scriptService.compile(script, DoubleValuesScript.CONTEXT).newInstance();

        assertEquals(1, result.execute(), 0.0001);
        assertEquals(0, result.variables().length);
    }
}
