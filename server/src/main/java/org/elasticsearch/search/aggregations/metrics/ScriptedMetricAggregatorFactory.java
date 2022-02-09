/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ScriptedMetricAggregatorFactory extends AggregatorFactory {

    private final ScriptedMetricAggContexts.MapScript.Factory mapScript;
    private final Map<String, Object> mapScriptParams;
    private final ScriptedMetricAggContexts.CombineScript.Factory combineScript;
    private final Map<String, Object> combineScriptParams;
    private final Script reduceScript;
    private final Map<String, Object> aggParams;
    @Nullable
    private final ScriptedMetricAggContexts.InitScript.Factory initScript;
    private final Map<String, Object> initScriptParams;

    ScriptedMetricAggregatorFactory(
        String name,
        ScriptedMetricAggContexts.MapScript.Factory mapScript,
        Map<String, Object> mapScriptParams,
        @Nullable ScriptedMetricAggContexts.InitScript.Factory initScript,
        Map<String, Object> initScriptParams,
        ScriptedMetricAggContexts.CombineScript.Factory combineScript,
        Map<String, Object> combineScriptParams,
        Script reduceScript,
        Map<String, Object> aggParams,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.mapScript = mapScript;
        this.mapScriptParams = mapScriptParams;
        this.initScript = initScript;
        this.initScriptParams = initScriptParams;
        this.combineScript = combineScript;
        this.combineScriptParams = combineScriptParams;
        this.reduceScript = reduceScript;
        this.aggParams = aggParams;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        Map<String, Object> aggParams = this.aggParams == null ? Map.of() : this.aggParams;

        Script reduceScript = deepCopyScript(this.reduceScript, aggParams);

        return new ScriptedMetricAggregator(
            name,
            context.lookup(),
            aggParams,
            initScript,
            initScriptParams,
            mapScript,
            mapScriptParams,
            combineScript,
            combineScriptParams,
            reduceScript,
            context,
            parent,
            metadata
        );
    }

    private static Script deepCopyScript(Script script, Map<String, Object> aggParams) {
        if (script != null) {
            Map<String, Object> params = mergeParams(aggParams, deepCopyParams(script.getParams()));
            return new Script(script.getType(), script.getLang(), script.getIdOrCode(), params);
        } else {
            return null;
        }
    }

    @SuppressWarnings({ "unchecked" })
    static <T> T deepCopyParams(T original) {
        T clone;
        if (original instanceof Map<?, ?> originalMap) {
            Map<Object, Object> clonedMap = new HashMap<>();
            for (Map.Entry<?, ?> e : originalMap.entrySet()) {
                clonedMap.put(deepCopyParams(e.getKey()), deepCopyParams(e.getValue()));
            }
            clone = (T) clonedMap;
        } else if (original instanceof List<?> originalList) {
            List<Object> clonedList = new ArrayList<>();
            for (Object o : originalList) {
                clonedList.add(deepCopyParams(o));
            }
            clone = (T) clonedList;
        } else if (original instanceof String
            || original instanceof Integer
            || original instanceof Long
            || original instanceof Short
            || original instanceof Byte
            || original instanceof Float
            || original instanceof Double
            || original instanceof Character
            || original instanceof Boolean) {
                clone = original;
            } else {
                throw new IllegalArgumentException(
                    "Can only clone primitives, String, ArrayList, and HashMap. Found: " + original.getClass().getCanonicalName()
                );
            }
        return clone;
    }

    static Map<String, Object> mergeParams(Map<String, Object> agg, Map<String, Object> script) {
        // Start with script params
        Map<String, Object> combined = new HashMap<>(script);

        // Add in agg params, throwing an exception if any conflicts are detected
        for (Map.Entry<String, Object> aggEntry : agg.entrySet()) {
            if (combined.putIfAbsent(aggEntry.getKey(), aggEntry.getValue()) != null) {
                throw new IllegalArgumentException(
                    "Parameter name \"" + aggEntry.getKey() + "\" used in both aggregation and script parameters"
                );
            }
        }

        return combined;
    }
}
