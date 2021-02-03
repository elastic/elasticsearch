/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public abstract class FieldScriptTestCase<T> extends ESTestCase {
    protected abstract ScriptContext<T> context();

    protected abstract T dummyScript();

    public final void testRateLimitingDisabled() throws IOException {
        try (ScriptService scriptService = TestScriptEngine.scriptService(context(), dummyScript())) {
            for (int i = 0; i < 1000; i++) {
                scriptService.compile(new Script(ScriptType.INLINE, "test", "test_" + i, Map.of()), context());
            }
        }
    }
}
