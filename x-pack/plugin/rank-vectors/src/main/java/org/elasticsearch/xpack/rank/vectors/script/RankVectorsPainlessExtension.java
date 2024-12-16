/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.script;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.Map;

public class RankVectorsPainlessExtension implements PainlessExtension {
    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(
        RankVectorsPainlessExtension.class,
        "rank_vector_whitelist.txt"
    );

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.of(ScoreScript.CONTEXT, List.of(WHITELIST));
    }
}
