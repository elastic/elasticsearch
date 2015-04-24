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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.*;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ScriptedMetricAggregator extends MetricsAggregator {

    private final String scriptLang;
    private final SearchScript mapScript;
    private final ExecutableScript combineScript;
    private final String reduceScript;
    // initial parameters for same shard scripts {init, map, combine}
    // state can be passed in params between them too
    private final Map<String, Object> params;
    // initial parameters for {reduce}
    private final Map<String, Object> reduceParams;
    private final ScriptType reduceScriptType;

    protected ScriptedMetricAggregator(String name, String scriptLang, ScriptType initScriptType, String initScript,
            ScriptType mapScriptType, String mapScript, ScriptType combineScriptType, String combineScript, ScriptType reduceScriptType,
            String reduceScript, Map<String, Object> params, Map<String, Object> reduceParams, AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, metaData);
        this.scriptLang = scriptLang;
        this.reduceScriptType = reduceScriptType;
        if (params == null) {
            this.params = new HashMap<>();
            this.params.put("_agg", new HashMap<>());
        } else {
            this.params = new HashMap<>(params);
        }
        if (reduceParams == null) {
            this.reduceParams = new HashMap<>();
        } else {
            this.reduceParams = reduceParams;
        }
        ScriptService scriptService = context.searchContext().scriptService();
        if (initScript != null) {
            scriptService.executable(new Script(scriptLang, initScript, initScriptType, this.params), ScriptContext.Standard.AGGS).run();
        }
        this.mapScript = scriptService.search(context.searchContext().lookup(), new Script(scriptLang, mapScript, mapScriptType, this.params), ScriptContext.Standard.AGGS);
        if (combineScript != null) {
            this.combineScript = scriptService.executable(new Script(scriptLang, combineScript, combineScriptType, this.params), ScriptContext.Standard.AGGS);
        } else {
            this.combineScript = null;
        }
        this.reduceScript = reduceScript;
    }

    @Override
    public boolean needsScores() {
        return true; // TODO: how can we know if the script relies on scores?
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final LeafSearchScript leafMapScript = mapScript.getLeafSearchScript(ctx);
        return new LeafBucketCollectorBase(sub, mapScript) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0 : bucket;
                leafMapScript.setDocument(doc);
                leafMapScript.run();
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object aggregation;
        if (combineScript != null) {
            aggregation = combineScript.run();
        } else {
            aggregation = params.get("_agg");
        }
        return new InternalScriptedMetric(name, aggregation, scriptLang, reduceScriptType, reduceScript, reduceParams, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, null, scriptLang, reduceScriptType, reduceScript, reduceParams, metaData());
    }

    public static class Factory extends AggregatorFactory {

        private String scriptLang;
        private ScriptType initScriptType;
        private ScriptType mapScriptType;
        private ScriptType combineScriptType;
        private ScriptType reduceScriptType;
        private String initScript;
        private String mapScript;
        private String combineScript;
        private String reduceScript;
        private Map<String, Object> params;
        private Map<String, Object> reduceParams;

        public Factory(String name, String scriptLang, ScriptType initScriptType, String initScript, ScriptType mapScriptType, String mapScript, ScriptType combineScriptType, String combineScript, ScriptType reduceScriptType, String reduceScript,
                Map<String, Object> params, Map<String, Object> reduceParams) {
            super(name, InternalScriptedMetric.TYPE.name());
            this.scriptLang = scriptLang;
            this.initScriptType = initScriptType;
            this.mapScriptType = mapScriptType;
            this.combineScriptType = combineScriptType;
            this.reduceScriptType = reduceScriptType;
            this.initScript = initScript;
            this.mapScript = mapScript;
            this.combineScript = combineScript;
            this.reduceScript = reduceScript;
            this.params = params;
            this.reduceParams = reduceParams;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, context, parent);
            }
            Map<String, Object> params = null;
            if (this.params != null) {
                params = deepCopyParams(this.params, context.searchContext());
            }
            Map<String, Object> reduceParams = null;
            if (this.reduceParams != null) {
                reduceParams = deepCopyParams(this.reduceParams, context.searchContext());
            }
            return new ScriptedMetricAggregator(name, scriptLang, initScriptType, initScript, mapScriptType, mapScript, combineScriptType,
                    combineScript, reduceScriptType, reduceScript, params, reduceParams, context, parent, metaData);
        }
        
        @SuppressWarnings({ "unchecked" })
        private static <T> T deepCopyParams(T original, SearchContext context) {
            T clone;
            if (original instanceof Map) {
                Map<?, ?> originalMap = (Map<?, ?>) original;
                Map<Object, Object> clonedMap = new HashMap<>();
                for (Entry<?, ?> e : originalMap.entrySet()) {
                    clonedMap.put(deepCopyParams(e.getKey(), context), deepCopyParams(e.getValue(), context));
                }
                clone = (T) clonedMap;
            } else if (original instanceof List) {
                List<?> originalList = (List<?>) original;
                List<Object> clonedList = new ArrayList<Object>();
                for (Object o : originalList) {
                    clonedList.add(deepCopyParams(o, context));
                }
                clone = (T) clonedList;
            } else if (original instanceof String || original instanceof Integer || original instanceof Long || original instanceof Short
                    || original instanceof Byte || original instanceof Float || original instanceof Double || original instanceof Character
                    || original instanceof Boolean) {
                clone = original;
            } else {
                throw new SearchParseException(context, "Can only clone primitives, String, ArrayList, and HashMap. Found: "
                        + original.getClass().getCanonicalName());
            }
            return clone;
        }

    }

}
