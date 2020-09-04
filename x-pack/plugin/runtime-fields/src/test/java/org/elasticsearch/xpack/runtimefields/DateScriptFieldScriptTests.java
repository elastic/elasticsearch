/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class DateScriptFieldScriptTests extends ScriptFieldScriptTestCase<DateScriptFieldScript.Factory> {
    public static final DateScriptFieldScript.Factory DUMMY = (params, lookup, formatter) -> ctx -> new DateScriptFieldScript(
        params,
        lookup,
        formatter,
        ctx
    ) {
        @Override
        public void execute() {
            emitValue(1595431354874L);
        }
    };

    @Override
    protected ScriptContext<DateScriptFieldScript.Factory> context() {
        return DateScriptFieldScript.CONTEXT;
    }

    @Override
    protected DateScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }
}
