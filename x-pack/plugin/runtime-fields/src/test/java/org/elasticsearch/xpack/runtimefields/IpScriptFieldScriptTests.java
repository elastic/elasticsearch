/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class IpScriptFieldScriptTests extends ScriptFieldScriptTestCase<IpScriptFieldScript.Factory> {
    public static final IpScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new IpScriptFieldScript(params, lookup, ctx) {
        @Override
        public void execute() {
            new IpScriptFieldScript.StringValue(this).stringValue("192.168.0.1");
        }
    };

    @Override
    protected ScriptContext<IpScriptFieldScript.Factory> context() {
        return IpScriptFieldScript.CONTEXT;
    }

    @Override
    protected IpScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }
}
