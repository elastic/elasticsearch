/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.murmur3;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.script.ScriptModule.CORE_CONTEXTS;

public class Murmur3PainlessExtension implements PainlessExtension {

    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(
        Murmur3PainlessExtension.class,
        "org.elasticsearch.field.murmur3.txt"
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
