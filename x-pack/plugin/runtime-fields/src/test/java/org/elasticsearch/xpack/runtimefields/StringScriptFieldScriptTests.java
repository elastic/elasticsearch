/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class StringScriptFieldScriptTests extends ESTestCase {
    public void testTooManyValues() {
        StringScriptFieldScript script = new StringScriptFieldScript(Map.of(), mock(SearchLookup.class), null) {
            @Override
            public void execute() {
                StringScriptFieldScript.Value value = new StringScriptFieldScript.Value(this);
                for (int i = 0; i <= AbstractScriptFieldScript.MAX_VALUES; i++) {
                    value.value("test");
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
                StringScriptFieldScript.Value value = new StringScriptFieldScript.Value(this);
                StringBuilder big = new StringBuilder();
                while (big.length() < StringScriptFieldScript.MAX_CHARS / 4) {
                    big.append("test");
                }
                String bigString = big.toString();
                for (int i = 0; i <= 4; i++) {
                    value.value(bigString);
                }
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, script::execute);
        assertThat(e.getMessage(), startsWith("too many characters in runtime values ["));
    }
}
