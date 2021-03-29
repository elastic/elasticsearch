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
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.ScriptContext;

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
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new WhitelistInstanceBinding(
                    AbstractFieldScript.class.getCanonicalName(),
                    plugin.grokHelper(),
                    "grok",
                    NamedGroupExtractor.class.getName(),
                    List.of(String.class.getName()),
                    List.of(CompileTimeOnlyAnnotation.INSTANCE)
                )
            )
        );
        this.whitelists = List.of(commonWhitelist, grokWhitelist);
    }

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.ofEntries(
            Map.entry(BooleanFieldScript.CONTEXT, whitelists),
            Map.entry(DateFieldScript.CONTEXT, whitelists),
            Map.entry(DoubleFieldScript.CONTEXT, whitelists),
            Map.entry(GeoPointFieldScript.CONTEXT, whitelists),
            Map.entry(IpFieldScript.CONTEXT, whitelists),
            Map.entry(LongFieldScript.CONTEXT, whitelists),
            Map.entry(StringFieldScript.CONTEXT, whitelists)
        );
    }
}
