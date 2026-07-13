/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.script.ScriptModule.CORE_CONTEXTS;

public class WildcardPainlessExtension implements PainlessExtension {
    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(
        WildcardPainlessExtension.class,
        "org.elasticsearch.xpack.wildcard.txt"
    );

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        List<Whitelist> whitelist = singletonList(WHITELIST);
        Map<ScriptContext<?>, List<Whitelist>> contextWhitelists = new HashMap<>(CORE_CONTEXTS.size());
        for (ScriptContext<?> scriptContext : CORE_CONTEXTS.values()) {
            contextWhitelists.put(scriptContext, whitelist);
        }
        return contextWhitelists;
    }
}
