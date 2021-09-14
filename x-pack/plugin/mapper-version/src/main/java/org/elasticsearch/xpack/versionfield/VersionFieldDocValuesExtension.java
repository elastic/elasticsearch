/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringSortScript;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class VersionFieldDocValuesExtension implements PainlessExtension {

    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(VersionFieldDocValuesExtension.class, "whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> whitelist = new HashMap<>();
        List<Whitelist> list = singletonList(WHITELIST);
        whitelist.put(AggregationScript.CONTEXT, list);
        whitelist.put(ScoreScript.CONTEXT, list);
        whitelist.put(FilterScript.CONTEXT, list);
        whitelist.put(FieldScript.CONTEXT, list);
        whitelist.put(NumberSortScript.CONTEXT, list);
        whitelist.put(StringSortScript.CONTEXT, list);
        return whitelist;
    }
}
