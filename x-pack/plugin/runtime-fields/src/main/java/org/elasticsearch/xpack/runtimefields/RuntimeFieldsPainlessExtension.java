/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.Map;

public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.ofEntries(
            Map.entry(DateScriptFieldScript.CONTEXT, DateScriptFieldScript.whitelist()),
            Map.entry(DoubleScriptFieldScript.CONTEXT, DoubleScriptFieldScript.whitelist()),
            Map.entry(IpScriptFieldScript.CONTEXT, IpScriptFieldScript.whitelist()),
            Map.entry(LongScriptFieldScript.CONTEXT, LongScriptFieldScript.whitelist()),
            Map.entry(StringScriptFieldScript.CONTEXT, StringScriptFieldScript.whitelist())
        );
    }
}
