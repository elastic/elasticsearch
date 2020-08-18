/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class DateScriptFieldScriptTests extends ESTestCase {
    public static final DateScriptFieldScript.Factory DUMMY = (params, lookup, formatter) -> ctx -> new DateScriptFieldScript(
        params,
        lookup,
        formatter,
        ctx
    ) {
        @Override
        public void execute() {
            new DateScriptFieldScript.Millis(this).millis(1595431354874L);
        }
    };

    public void testRateLimitingDisabled() throws IOException {
        try (ScriptService scriptService = TestScriptEngine.scriptService(DateScriptFieldScript.CONTEXT, DUMMY)) {
            for (int i = 0; i < 1000; i++) {
                scriptService.compile(new Script(ScriptType.INLINE, "test", "test_" + i, Map.of()), DateScriptFieldScript.CONTEXT);
            }
        }
    }
}
