/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        return org.elasticsearch.common.collect.Map.of(
            BooleanFieldScript.CONTEXT,
            BooleanFieldScript.whitelist(),
            DateFieldScript.CONTEXT,
            DateFieldScript.whitelist(),
            DoubleFieldScript.CONTEXT,
            DoubleFieldScript.whitelist(),
            GeoPointFieldScript.CONTEXT,
            GeoPointFieldScript.whitelist(),
            IpFieldScript.CONTEXT,
            IpFieldScript.whitelist(),
            LongFieldScript.CONTEXT,
            LongFieldScript.whitelist(),
            StringFieldScript.CONTEXT,
            StringFieldScript.whitelist()
        );
    }
}
