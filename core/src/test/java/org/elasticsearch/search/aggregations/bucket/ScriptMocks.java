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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper mocks for script plugins and engines
 */
public class ScriptMocks {

    /**
     * Mock plugin for the {@link ScriptMocks.ExtractFieldScriptEngine}
     */
    public static class ExtractFieldScriptPlugin extends Plugin {

        @Override
        public String name() {
            return ScriptMocks.ExtractFieldScriptEngine.NAME;
        }

        @Override
        public String description() {
            return "Mock script engine for " + DateHistogramIT.class;
        }

        public void onModule(ScriptModule module) {
            module.addScriptEngine(new ScriptEngineRegistry.ScriptEngineRegistration(ScriptMocks.ExtractFieldScriptEngine.class,
                   ScriptMocks.ExtractFieldScriptEngine.TYPES));
        }

    }

    /**
     * This mock script returns the field that is specified by name in the script body
     */
    public static class ExtractFieldScriptEngine implements ScriptEngineService {

        public static final String NAME = "extract_field";

        public static final List<String> TYPES = Collections.singletonList(NAME);

        @Override
        public void close() throws IOException {
        }

        @Override
        public List<String> getTypes() {
            return TYPES;
        }

        @Override
        public List<String> getExtensions() {
            return TYPES;
        }

        @Override
        public boolean isSandboxed() {
            return true;
        }

        @Override
        public Object compile(String script, Map<String, String> params) {
            return script;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }
        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            return new SearchScript() {

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {
                        @Override
                        public void setNextVar(String name, Object value) {
                        }

                        @Override
                        public Object run() {
                            String fieldName = (String) compiledScript.compiled();
                            return leafLookup.doc().get(fieldName);
                        }

                        @Override
                        public void setScorer(Scorer scorer) {
                        }

                        @Override
                        public void setSource(Map<String, Object> source) {
                        }

                        @Override
                        public void setDocument(int doc) {
                            if (leafLookup != null) {
                                leafLookup.setDocument(doc);
                            }
                        }

                        @Override
                        public long runAsLong() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public float runAsFloat() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public double runAsDouble() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            };
        }

        @Override
        public void scriptRemoved(CompiledScript script) {
        }
    }

    /**
     * Mock plugin for the {@link ScriptMocks.FieldValueScriptEngine}
     */
    public static class FieldValueScriptPlugin extends Plugin {

        @Override
        public String name() {
            return ScriptMocks.FieldValueScriptEngine.NAME;
        }

        @Override
        public String description() {
            return "Mock script engine for " + DateHistogramIT.class;
        }

        public void onModule(ScriptModule module) {
            module.addScriptEngine(new ScriptEngineRegistry.ScriptEngineRegistration(ScriptMocks.FieldValueScriptEngine.class,
                   ScriptMocks.FieldValueScriptEngine.TYPES));
        }

    }

    /**
     * This mock script returns the field value and adds one month to the returned date
     */
    public static class FieldValueScriptEngine implements ScriptEngineService {

        public static final String NAME = "field_value";

        public static final List<String> TYPES = Collections.singletonList(NAME);

        @Override
        public void close() throws IOException {
        }

        @Override
        public List<String> getTypes() {
            return TYPES;
        }

        @Override
        public List<String> getExtensions() {
            return TYPES;
        }

        @Override
        public boolean isSandboxed() {
            return true;
        }

        @Override
        public Object compile(String script, Map<String, String> params) {
            return script;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }
        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            return new SearchScript() {

                private Map<String, Object> vars = new HashMap<>(2);

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {

                        @Override
                        public Object unwrap(Object value) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setNextVar(String name, Object value) {
                            vars.put(name, value);
                        }

                        @Override
                        public Object run() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setScorer(Scorer scorer) {
                        }

                        @Override
                        public void setSource(Map<String, Object> source) {
                        }

                        @Override
                        public void setDocument(int doc) {
                            if (leafLookup != null) {
                                leafLookup.setDocument(doc);
                            }
                        }

                        @Override
                        public long runAsLong() {
                            return new DateTime((long) vars.get("_value"), DateTimeZone.UTC).plusMonths(1).getMillis();
                        }

                        @Override
                        public float runAsFloat() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public double runAsDouble() {
                            return new DateTime(new Double((double) vars.get("_value")).longValue(),
                                        DateTimeZone.UTC).plusMonths(1).getMillis();
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            };
        }

        @Override
        public void scriptRemoved(CompiledScript script) {
        }
    }

}
