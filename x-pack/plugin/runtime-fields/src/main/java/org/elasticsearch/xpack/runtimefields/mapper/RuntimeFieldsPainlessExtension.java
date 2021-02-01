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
    }

    private List<Whitelist> load(String path) {
        return List.of(commonWhitelist, grokWhitelist, WhitelistLoader.loadFromResourceFiles(AbstractFieldScript.class, path));
    }

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.ofEntries(
            Map.entry(BooleanFieldScript.CONTEXT, load("boolean_whitelist.txt")),
            Map.entry(DateFieldScript.CONTEXT, load("date_whitelist.txt")),
            Map.entry(DoubleFieldScript.CONTEXT, load("double_whitelist.txt")),
            Map.entry(GeoPointFieldScript.CONTEXT, load("geo_point_whitelist.txt")),
            Map.entry(IpFieldScript.CONTEXT, load("ip_whitelist.txt")),
            Map.entry(LongFieldScript.CONTEXT, load("long_whitelist.txt")),
            Map.entry(StringFieldScript.CONTEXT, load("string_whitelist.txt"))
        );
    }
}
