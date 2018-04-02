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

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;

public class MetricAggScripts {
    private abstract static class ParamsAndAggBase {
        private final Map<String, Object> params;
        private final Object agg;

        ParamsAndAggBase(Map<String, Object> params, Object agg) {
            this.params = params;
            this.agg = agg;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public Object getAgg() {
            return agg;
        }
    }

    public abstract static class InitScript extends ParamsAndAggBase {
        public InitScript(Map<String, Object> params, Object agg) {
            super(params, agg);
        }

        public abstract void execute();

        public interface Factory {
            InitScript newInstance(Map<String, Object> params, Object agg);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_init", Factory.class);
    }

    public abstract static class MapScript extends ParamsAndAggBase {
        private final LeafSearchLookup leafLookup;

        public MapScript(Map<String, Object> params, Object agg, SearchLookup lookup, LeafReaderContext leafContext) {
            super(params, agg);

            this.leafLookup = leafContext == null ? null : lookup.getLeafSearchLookup(leafContext);
        }

        // Return the doc as a map (instead of LeafDocLookup) in order to abide by type whitelisting rules for
        // Painless scripts.
        public Map<String, ScriptDocValues<?>> getDoc() {
            return leafLookup == null ? null : leafLookup.doc();
        }

        public void setDocument(int docId) {
            if (leafLookup != null) {
                leafLookup.setDocument(docId);
            }
        }

        public abstract void execute(double _score);

        public interface LeafFactory {
            MapScript newInstance(LeafReaderContext ctx);
        }

        public interface Factory {
            LeafFactory newFactory(Map<String, Object> params, Object agg, SearchLookup lookup);
        }

        public static String[] PARAMETERS = new String[] {"_score"};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_map", Factory.class);
    }

    public abstract static class CombineScript extends ParamsAndAggBase {
        public CombineScript(Map<String, Object> params, Object agg) {
            super(params, agg);
        }

        public abstract Object execute();

        public interface Factory {
            CombineScript newInstance(Map<String, Object> params, Object agg);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_combine", Factory.class);
    }

    public abstract static class ReduceScript {
        private final Map<String, Object> params;
        private final List<Object> aggs;

        public ReduceScript(Map<String, Object> params, List<Object> aggs) {
            this.params = params;
            this.aggs = aggs;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public List<Object> getAggs() {
            return aggs;
        }

        public abstract Object execute();

        public interface Factory {
            ReduceScript newInstance(Map<String, Object> params, List<Object> aggs);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_reduce", Factory.class);
    }
}
