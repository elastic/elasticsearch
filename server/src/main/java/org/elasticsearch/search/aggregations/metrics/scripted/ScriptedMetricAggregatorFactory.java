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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.SearchScript;
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
import java.util.function.Function;

public class ScriptedMetricAggregatorFactory extends AggregatorFactory<ScriptedMetricAggregatorFactory> {

    private final SearchScript.Factory mapScript;
    private final ExecutableScript.Factory combineScript;
    private final Script reduceScript;
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private final ExecutableScript.Factory initScript;

    public ScriptedMetricAggregatorFactory(String name, SearchScript.Factory mapScript, ExecutableScript.Factory initScript,
                                           ExecutableScript.Factory combineScript, Script reduceScript, Map<String, Object> params,
                                           SearchLookup lookup, SearchContext context, AggregatorFactory<?> parent,
                                           AggregatorFactories.Builder subFactories, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, subFactories, metaData);
        this.mapScript = mapScript;
        this.initScript = initScript;
        this.combineScript = combineScript;
        this.reduceScript = reduceScript;
        this.lookup = lookup;
        this.params = params;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        Map<String, Object> params = this.params;
        if (params != null) {
            params = deepCopyParams(params, context);
        } else {
            params = new HashMap<>();
        }
        if (params.containsKey("_agg") == false) {
            params.put("_agg", new HashMap<String, Object>());
        }

        final ExecutableScript initScript = this.initScript.newInstance(params);
        final SearchScript.LeafFactory mapScript = this.mapScript.newFactory(params, lookup);
        final ExecutableScript combineScript = this.combineScript.newInstance(params);

        final Script reduceScript = deepCopyScript(this.reduceScript, context);
        if (initScript != null) {
            initScript.run();
        }
        return new ScriptedMetricAggregator(name, mapScript,
                combineScript, reduceScript, params, context, parent,
                pipelineAggregators, metaData);
    }

    private static Script deepCopyScript(Script script, SearchContext context) {
        if (script != null) {
            Map<String, Object> params = script.getParams();
            if (params != null) {
                params = deepCopyParams(params, context);
            }
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
            throw new SearchParseException(context,
                "Can only clone primitives, String, ArrayList, and HashMap. Found: " + original.getClass().getCanonicalName(), null);
        }
        return clone;
    }


}
