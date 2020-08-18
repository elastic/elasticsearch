/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class StringScriptFieldScriptTests extends ScriptFieldScriptTestCase<StringScriptFieldScript.Factory> {
    public static final StringScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new StringScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            new StringScriptFieldScript.Value(this).value("foo");
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
}
