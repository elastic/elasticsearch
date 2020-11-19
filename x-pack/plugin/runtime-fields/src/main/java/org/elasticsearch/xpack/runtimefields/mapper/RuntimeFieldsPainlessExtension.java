/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.Map;

public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.ofEntries(
            Map.entry(BooleanFieldScript.CONTEXT, BooleanFieldScript.whitelist()),
            Map.entry(DateFieldScript.CONTEXT, DateFieldScript.whitelist()),
            Map.entry(DoubleFieldScript.CONTEXT, DoubleFieldScript.whitelist()),
            Map.entry(GeoPointFieldScript.CONTEXT, GeoPointFieldScript.whitelist()),
            Map.entry(IpFieldScript.CONTEXT, IpFieldScript.whitelist()),
            Map.entry(LongFieldScript.CONTEXT, LongFieldScript.whitelist()),
            Map.entry(StringFieldScript.CONTEXT, StringFieldScript.whitelist())
        );
    }
}
