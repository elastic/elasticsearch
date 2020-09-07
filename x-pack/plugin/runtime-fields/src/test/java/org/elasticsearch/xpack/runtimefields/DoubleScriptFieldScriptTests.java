/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class DoubleScriptFieldScriptTests extends ScriptFieldScriptTestCase<DoubleScriptFieldScript.Factory> {
    public static final DoubleScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new DoubleScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emitValue(1.0);
        }
    };

    @Override
    protected ScriptContext<DoubleScriptFieldScript.Factory> context() {
        return DoubleScriptFieldScript.CONTEXT;
    }

    @Override
    protected DoubleScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }
}
