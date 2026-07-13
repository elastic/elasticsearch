/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpatialPainlessExtension implements PainlessExtension {

    private static final List<Whitelist> WHITELISTS = List.of(
        WhitelistLoader.loadFromResourceFiles(
            SpatialPainlessExtension.class,
            "org.elasticsearch.xpack.spatial.index.fielddata.txt",
            "org.elasticsearch.xpack.spatial.index.fielddata.plain.txt",
            "org.elasticsearch.xpack.spatial.index.mapper.txt"
        )
    );

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> contextWhitelistMap = new HashMap<>();

        for (ScriptContext<?> context : ScriptModule.CORE_CONTEXTS.values()) {
            contextWhitelistMap.put(context, WHITELISTS);
        }

        return contextWhitelistMap;
    }
}
