/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class BooleanScriptFieldScriptTests extends ScriptFieldScriptTestCase<BooleanScriptFieldScript.Factory> {
    public static final BooleanScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new BooleanScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emitValue(false);
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
}
