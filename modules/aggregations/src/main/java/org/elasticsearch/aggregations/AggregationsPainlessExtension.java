/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations;

import org.elasticsearch.aggregations.pipeline.MovingFunctionScript;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;

import java.util.List;
import java.util.Map;

/**
 * Extends the painless whitelist for the {@link MovingFunctionScript} to include {@link MovingFunctions}.
 */
public class AggregationsPainlessExtension implements PainlessExtension {
    private static final Whitelist MOVING_FUNCTION_ALLOWLIST = WhitelistLoader.loadFromResourceFiles(
        AggregationsPainlessExtension.class,
        "moving_function_whitelist.txt"
    );

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Map.ofEntries(
            Map.entry(MovingFunctionScript.CONTEXT, List.of(MOVING_FUNCTION_ALLOWLIST)),
            Map.entry(PainlessTestScript.CONTEXT, List.of(MOVING_FUNCTION_ALLOWLIST))
        );
    }
}
