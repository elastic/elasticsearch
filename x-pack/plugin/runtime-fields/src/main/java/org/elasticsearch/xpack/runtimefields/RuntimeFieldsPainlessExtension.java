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
            Map.entry(BooleanScript.CONTEXT, BooleanScript.whitelist()),
            Map.entry(DateScript.CONTEXT, DateScript.whitelist()),
            Map.entry(DoubleScript.CONTEXT, DoubleScript.whitelist()),
            Map.entry(IpScript.CONTEXT, IpScript.whitelist()),
            Map.entry(LongScript.CONTEXT, LongScript.whitelist()),
            Map.entry(StringScript.CONTEXT, StringScript.whitelist())
        );
    }
}
