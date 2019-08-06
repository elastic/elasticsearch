/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.vectors.query;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class DocValuesWhitelistExtension implements PainlessExtension {

    private static final Whitelist WHITELIST =
        WhitelistLoader.loadFromResourceFiles(DocValuesWhitelistExtension.class, "whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> whitelist = new HashMap<>();
        List<Whitelist> list = singletonList(WHITELIST);
        whitelist.put(ScoreScript.CONTEXT, list);
        whitelist.put(NumberSortScript.CONTEXT, list);
        return whitelist;
    }
}
