/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.runtimefields.mapper.AbstractFieldScript;
import org.elasticsearch.runtimefields.mapper.BooleanFieldScript;
import org.elasticsearch.runtimefields.mapper.DateFieldScript;
import org.elasticsearch.runtimefields.mapper.DoubleFieldScript;
import org.elasticsearch.runtimefields.mapper.GeoPointFieldScript;
import org.elasticsearch.runtimefields.mapper.IpFieldScript;
import org.elasticsearch.runtimefields.mapper.LongFieldScript;
import org.elasticsearch.runtimefields.mapper.StringFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;

import java.util.List;
import java.util.Map;

public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    private final List<Whitelist> whitelists;

    public RuntimeFieldsPainlessExtension(RuntimeFields plugin) {
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
