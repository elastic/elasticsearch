/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<LongScriptFieldScript.Factory> {
    public static final LongScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new LongScriptFieldScript(params, lookup, ctx) {
        @Override
        public void execute() {
            new LongScriptFieldScript.Value(this).value(1);
        }
    };

    @Override
    protected ScriptContext<LongScriptFieldScript.Factory> context() {
        return LongScriptFieldScript.CONTEXT;
    }

    @Override
    protected LongScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }
}
