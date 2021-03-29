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
            org.elasticsearch.common.collect.List.of(),
            org.elasticsearch.common.collect.List.of(),
            org.elasticsearch.common.collect.List.of(),
            org.elasticsearch.common.collect.List.of(
                new WhitelistInstanceBinding(
                    AbstractFieldScript.class.getCanonicalName(),
                    plugin.grokHelper(),
                    "grok",
                    NamedGroupExtractor.class.getName(),
                    org.elasticsearch.common.collect.List.of(String.class.getName()),
                    org.elasticsearch.common.collect.List.of(CompileTimeOnlyAnnotation.INSTANCE)
                )
            )
        );
        this.whitelists = org.elasticsearch.common.collect.List.of(commonWhitelist, grokWhitelist);
    }

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return org.elasticsearch.common.collect.Map.ofEntries(
            org.elasticsearch.common.collect.Map.entry(BooleanFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(DateFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(DoubleFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(GeoPointFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(IpFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(LongFieldScript.CONTEXT, whitelists),
            org.elasticsearch.common.collect.Map.entry(StringFieldScript.CONTEXT, whitelists)
        );
    }
}
