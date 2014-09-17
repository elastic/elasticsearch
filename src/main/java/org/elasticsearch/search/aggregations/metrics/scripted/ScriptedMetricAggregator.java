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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
    private ScriptService scriptService;
    private ScriptType scriptType;

    protected ScriptedMetricAggregator(String name, String scriptLang, ScriptType scriptType, String initScript, String mapScript,
            String combineScript, String reduceScript, Map<String, Object> params, Map<String, Object> reduceParams,
            AggregationContext context, Aggregator parent) {
        super(name, 1, context, parent);
        this.scriptService = context.searchContext().scriptService();
        this.scriptLang = scriptLang;
        this.scriptType = scriptType;
        if (params == null) {
            this.params = new HashMap<>();
            this.params.put("_agg", new HashMap<>());
        } else {
            this.params = params;
        }
        if (reduceParams == null) {
            this.reduceParams = new HashMap<>();
        } else {
            this.reduceParams = reduceParams;
        }
        if (initScript != null) {
            scriptService.executable(scriptLang, initScript, scriptType, this.params).run();
        }
        this.mapScript = scriptService.search(context.searchContext().lookup(), scriptLang, mapScript, scriptType, this.params);
        if (combineScript != null) {
            this.combineScript = scriptService.executable(scriptLang, combineScript, scriptType, this.params);
        } else {
            this.combineScript = null;
        }
        this.reduceScript = reduceScript;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        mapScript.setNextReader(reader);
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        mapScript.setNextDocId(docId);
        mapScript.run();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object aggregation;
        if (combineScript != null) {
            aggregation = combineScript.run();
        } else {
            aggregation = params.get("_agg");
        }
        return new InternalScriptedMetric(name, aggregation, scriptLang, scriptType, reduceScript, reduceParams);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, null, scriptLang, scriptType, reduceScript, reduceParams);
    }

    public static class Factory extends AggregatorFactory {

        private String scriptLang;
        private ScriptType scriptType;
        private String initScript;
        private String mapScript;
        private String combineScript;
        private String reduceScript;
        private Map<String, Object> params;
        private Map<String, Object> reduceParams;

        public Factory(String name, String scriptLang, ScriptType scriptType, String initScript, String mapScript, String combineScript, String reduceScript,
                Map<String, Object> params, Map<String, Object> reduceParams) {
            super(name, InternalScriptedMetric.TYPE.name());
            this.scriptLang = scriptLang;
            this.scriptType = scriptType;
            this.initScript = initScript;
            this.mapScript = mapScript;
            this.combineScript = combineScript;
            this.reduceScript = reduceScript;
            this.params = params;
            this.reduceParams = reduceParams;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            return new ScriptedMetricAggregator(name, scriptLang, scriptType, initScript, mapScript, combineScript, reduceScript, params,
                    reduceParams, context, parent);
        }

    }

}
