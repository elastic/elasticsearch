/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BooleanScriptFieldScriptTests extends ScriptFieldScriptTestCase<BooleanScriptFieldScript.Factory> {
    public static final BooleanScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new BooleanScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public boolean[] execute() {
            return new boolean[] { false };
        }
    };

    @Override
    protected ScriptContext<BooleanScriptFieldScript.Factory> context() {
        return BooleanScriptFieldScript.CONTEXT;
    }

    @Override
    protected BooleanScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testConvertBoolean() {
        boolean v = randomBoolean();
        assertThat(BooleanScriptFieldScript.convertFromBoolean(v), equalTo(new boolean[] { v }));
        assertThat(BooleanScriptFieldScript.convertFromDef(v), equalTo(new boolean[] { v }));
    }

    public void testConvertFromCollection() {
        boolean[] result = new boolean[] { randomBoolean(), randomBoolean(), randomBoolean() };
        assertThat(BooleanScriptFieldScript.convertFromCollection(List.of(result[0], result[1], result[2])), equalTo(result));
        assertThat(BooleanScriptFieldScript.convertFromDef(List.of(result[0], result[1], result[2])), equalTo(result));
    }

    public void testConvertBooleanArrayFromDef() {
        boolean[] result = new boolean[] { randomBoolean(), randomBoolean(), randomBoolean() };
        assertThat(BooleanScriptFieldScript.convertFromDef(result), equalTo(result));
    }
}
