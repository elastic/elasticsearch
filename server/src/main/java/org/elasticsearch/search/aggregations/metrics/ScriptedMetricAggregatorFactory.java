/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

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
    private final SearchLookup lookup;
    private final ScriptedMetricAggContexts.InitScript.Factory initScript;
    private final Map<String, Object> initScriptParams;

    ScriptedMetricAggregatorFactory(String name,
                                    ScriptedMetricAggContexts.MapScript.Factory mapScript, Map<String, Object> mapScriptParams,
                                    ScriptedMetricAggContexts.InitScript.Factory initScript, Map<String, Object> initScriptParams,
                                    ScriptedMetricAggContexts.CombineScript.Factory combineScript,
                                    Map<String, Object> combineScriptParams, Script reduceScript, Map<String, Object> aggParams,
                                    SearchLookup lookup, QueryShardContext queryShardContext, AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactories, Map<String, Object> metaData) throws IOException {
        super(name, queryShardContext, parent, subFactories, metaData);
        this.mapScript = mapScript;
        this.mapScriptParams = mapScriptParams;
        this.initScript = initScript;
        this.initScriptParams = initScriptParams;
        this.combineScript = combineScript;
        this.combineScriptParams = combineScriptParams;
        this.reduceScript = reduceScript;
        this.lookup = lookup;
        this.aggParams = aggParams;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        boolean collectsFromSingleBucket,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, searchContext, parent);
        }
        Map<String, Object> aggParams = this.aggParams;
        if (aggParams != null) {
            aggParams = deepCopyParams(aggParams, searchContext);
        } else {
            aggParams = new HashMap<>();
        }

        Map<String, Object> aggState = new HashMap<String, Object>();

        final ScriptedMetricAggContexts.InitScript initScript = this.initScript.newInstance(
            mergeParams(aggParams, initScriptParams), aggState);
        final ScriptedMetricAggContexts.MapScript.LeafFactory mapScript = this.mapScript.newFactory(
            mergeParams(aggParams, mapScriptParams), aggState, lookup);
        final ScriptedMetricAggContexts.CombineScript combineScript = this.combineScript.newInstance(
            mergeParams(aggParams, combineScriptParams), aggState);

        final Script reduceScript = deepCopyScript(this.reduceScript, searchContext, aggParams);
        if (initScript != null) {
            initScript.execute();
            CollectionUtils.ensureNoSelfReferences(aggState, "Scripted metric aggs init script");
        }
        return new ScriptedMetricAggregator(name, mapScript,
                combineScript, reduceScript, aggState, searchContext, parent,
                pipelineAggregators, metaData);
    }

    private static Script deepCopyScript(Script script, SearchContext context, Map<String, Object> aggParams) {
        if (script != null) {
            Map<String, Object> params = mergeParams(aggParams, deepCopyParams(script.getParams(), context));
            return new Script(script.getType(), script.getLang(), script.getIdOrCode(), params);
        } else {
            return null;
        }
    }

    @SuppressWarnings({ "unchecked" })
    private static <T> T deepCopyParams(T original, SearchContext context) {
        T clone;
        if (original instanceof Map) {
            Map<?, ?> originalMap = (Map<?, ?>) original;
            Map<Object, Object> clonedMap = new HashMap<>();
            for (Map.Entry<?, ?> e : originalMap.entrySet()) {
                clonedMap.put(deepCopyParams(e.getKey(), context), deepCopyParams(e.getValue(), context));
            }
            clone = (T) clonedMap;
        } else if (original instanceof List) {
            List<?> originalList = (List<?>) original;
            List<Object> clonedList = new ArrayList<>();
            for (Object o : originalList) {
                clonedList.add(deepCopyParams(o, context));
            }
            clone = (T) clonedList;
        } else if (original instanceof String || original instanceof Integer || original instanceof Long || original instanceof Short
            || original instanceof Byte || original instanceof Float || original instanceof Double || original instanceof Character
            || original instanceof Boolean) {
            clone = original;
        } else {
            throw new SearchParseException(context.shardTarget(),
                "Can only clone primitives, String, ArrayList, and HashMap. Found: " + original.getClass().getCanonicalName(), null);
        }
        return clone;
    }

    private static Map<String, Object> mergeParams(Map<String, Object> agg, Map<String, Object> script) {
        // Start with script params
        Map<String, Object> combined = new HashMap<>(script);

        // Add in agg params, throwing an exception if any conflicts are detected
        for (Map.Entry<String, Object> aggEntry : agg.entrySet()) {
            if (combined.putIfAbsent(aggEntry.getKey(), aggEntry.getValue()) != null) {
                throw new IllegalArgumentException("Parameter name \"" + aggEntry.getKey() +
                    "\" used in both aggregation and script parameters");
            }
        }

        return combined;
    }
}
