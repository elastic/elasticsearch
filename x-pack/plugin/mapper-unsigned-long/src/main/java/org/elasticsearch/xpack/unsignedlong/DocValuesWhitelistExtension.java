/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.BucketAggregationSelectorScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.script.StringSortScript;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Map.entry;

public class DocValuesWhitelistExtension implements PainlessExtension {

    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(
        DocValuesWhitelistExtension.class,
        "org.elasticsearch.xpack.unsignedlong.txt"
    );

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        List<Whitelist> whitelist = singletonList(WHITELIST);
        return Map.ofEntries(
            entry(FieldScript.CONTEXT, whitelist),
            entry(ScoreScript.CONTEXT, whitelist),
            entry(FilterScript.CONTEXT, whitelist),
            entry(AggregationScript.CONTEXT, whitelist),
            entry(NumberSortScript.CONTEXT, whitelist),
            entry(StringSortScript.CONTEXT, whitelist),
            entry(BucketAggregationSelectorScript.CONTEXT, whitelist),
            entry(ScriptedMetricAggContexts.InitScript.CONTEXT, whitelist),
            entry(ScriptedMetricAggContexts.MapScript.CONTEXT, whitelist),
            entry(ScriptedMetricAggContexts.CombineScript.CONTEXT, whitelist),
            entry(ScriptedMetricAggContexts.ReduceScript.CONTEXT, whitelist)
        );
    }
}
