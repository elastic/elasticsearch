/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The painless extension that adds the necessary whitelist for grok and dissect to all the existing runtime fields contexts.
 */
public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    private final List<Whitelist> whitelists;

    public RuntimeFieldsPainlessExtension(RuntimeFieldsCommonPlugin plugin) {
        Whitelist commonWhitelist = WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "common_whitelist.txt");
        Whitelist grokWhitelist = new Whitelist(
            commonWhitelist.classLoader,
            org.elasticsearch.core.List.of(),
            org.elasticsearch.core.List.of(),
            org.elasticsearch.core.List.of(),
            org.elasticsearch.core.List.of(
                new WhitelistInstanceBinding(
                    AbstractFieldScript.class.getCanonicalName(),
                    plugin.grokHelper(),
                    "grok",
                    NamedGroupExtractor.class.getName(),
                    org.elasticsearch.core.List.of(String.class.getName()),
                    org.elasticsearch.core.List.of(CompileTimeOnlyAnnotation.INSTANCE)
                )
            )
        );
        this.whitelists = org.elasticsearch.core.List.of(commonWhitelist, grokWhitelist);
    }

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> whiteLists = new HashMap<>();
        for (ScriptContext<?> scriptContext : ScriptModule.RUNTIME_FIELDS_CONTEXTS) {
            whiteLists.put(scriptContext, whitelists);
        }
        return Collections.unmodifiableMap(whiteLists);
    }
}
