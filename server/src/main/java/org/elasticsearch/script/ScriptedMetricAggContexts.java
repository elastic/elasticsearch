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
import org.apache.lucene.search.Scorable;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ScriptedMetricAggContexts {
    private abstract static class ParamsAndStateBase {
        private final Map<String, Object> params;
        private final Map<String, Object> state;

        ParamsAndStateBase(Map<String, Object> params, Map<String, Object> state) {
            this.params = params;
            this.state = state;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public Object getState() {
            return state;
        }
    }

    public abstract static class InitScript extends ParamsAndStateBase {
        public InitScript(Map<String, Object> params, Map<String, Object> state) {
            super(params, state);
        }

        public abstract void execute();

        public interface Factory {
            InitScript newInstance(Map<String, Object> params, Map<String, Object> state);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_init", Factory.class);
    }

    public abstract static class MapScript extends ParamsAndStateBase {
        private final LeafSearchLookup leafLookup;
        private Scorable scorer;

        public MapScript(Map<String, Object> params, Map<String, Object> state, SearchLookup lookup, LeafReaderContext leafContext) {
            super(params, state);

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

        public void setScorer(Scorable scorer) {
            this.scorer = scorer;
        }

        // get_score() is named this way so that it's picked up by Painless as '_score'
        public double get_score() {
            if (scorer == null) {
                return 0.0;
            }

            try {
                return scorer.score();
            } catch (IOException e) {
                throw new ElasticsearchException("Couldn't look up score", e);
            }
        }

        public abstract void execute();

        public interface LeafFactory {
            MapScript newInstance(LeafReaderContext ctx);
        }

        public interface Factory {
            LeafFactory newFactory(Map<String, Object> params, Map<String, Object> state, SearchLookup lookup);
        }

        public static String[] PARAMETERS = new String[] {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_map", Factory.class);
    }

    public abstract static class CombineScript extends ParamsAndStateBase {
        public CombineScript(Map<String, Object> params, Map<String, Object> state) {
            super(params, state);
        }

        public abstract Object execute();

        public interface Factory {
            CombineScript newInstance(Map<String, Object> params, Map<String, Object> state);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_combine", Factory.class);
    }

    public abstract static class ReduceScript {
        private final Map<String, Object> params;
        private final List<Object> states;

        public ReduceScript(Map<String, Object> params, List<Object> states) {
            this.params = params;
            this.states = states;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public List<Object> getStates() {
            return states;
        }

        public abstract Object execute();

        public interface Factory {
            ReduceScript newInstance(Map<String, Object> params, List<Object> states);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_reduce", Factory.class);
    }
}
