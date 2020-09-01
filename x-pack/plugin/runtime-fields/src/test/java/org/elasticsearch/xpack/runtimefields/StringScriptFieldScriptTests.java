/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class StringScriptFieldScriptTests extends ScriptFieldScriptTestCase<StringScriptFieldScript.Factory> {
    public static final StringScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new StringScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emitValue("foo");
        }
    };

    @Override
    protected ScriptContext<StringScriptFieldScript.Factory> context() {
        return StringScriptFieldScript.CONTEXT;
    }

    @Override
    protected StringScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testTooManyValues() {
        StringScriptFieldScript script = new StringScriptFieldScript(Map.of(), mock(SearchLookup.class), null) {
            @Override
            public void execute() {
                for (int i = 0; i <= AbstractScriptFieldScript.MAX_VALUES; i++) {
                    emitValue("test");
                }
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, script::execute);
        assertThat(e.getMessage(), equalTo("too many runtime values"));
    }

    public void testTooManyChars() {
        StringScriptFieldScript script = new StringScriptFieldScript(Map.of(), mock(SearchLookup.class), null) {
            @Override
            public void execute() {
                StringBuilder big = new StringBuilder();
                while (big.length() < StringScriptFieldScript.MAX_CHARS / 4) {
                    big.append("test");
                }
                String bigString = big.toString();
                for (int i = 0; i <= 4; i++) {
                    emitValue(bigString);
                }
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, script::execute);
        assertThat(e.getMessage(), startsWith("too many characters in runtime values ["));
    }
}
