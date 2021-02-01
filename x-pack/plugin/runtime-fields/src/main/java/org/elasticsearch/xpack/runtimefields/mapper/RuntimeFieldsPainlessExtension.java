/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;

import java.util.List;
import java.util.Map;

public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    private final Whitelist commonWhitelist = WhitelistLoader.loadFromResourceFiles(AbstractFieldScript.class, "common_whitelist.txt");

    private final Whitelist grokWhitelist;

    public RuntimeFieldsPainlessExtension(RuntimeFields plugin) {
        grokWhitelist = new Whitelist(
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
    }

    private List<Whitelist> load(String path) {
        return org.elasticsearch.common.collect.List.of(
            commonWhitelist,
            grokWhitelist,
            WhitelistLoader.loadFromResourceFiles(AbstractFieldScript.class, path)
        );
    }

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return org.elasticsearch.common.collect.Map.ofEntries(
            org.elasticsearch.common.collect.Map.entry(BooleanFieldScript.CONTEXT, load("boolean_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(DateFieldScript.CONTEXT, load("date_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(DoubleFieldScript.CONTEXT, load("double_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(GeoPointFieldScript.CONTEXT, load("geo_point_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(IpFieldScript.CONTEXT, load("ip_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(LongFieldScript.CONTEXT, load("long_whitelist.txt")),
            org.elasticsearch.common.collect.Map.entry(StringFieldScript.CONTEXT, load("string_whitelist.txt"))
        );
    }
}
