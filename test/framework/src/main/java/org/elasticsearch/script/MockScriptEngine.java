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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Doc;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Field;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Query;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Term;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;

/**
 * A mocked script engine that can be used for testing purpose.
 *
 * This script engine allows to define a set of predefined scripts that basically a combination of a key and a
 * function:
 *
 * The key can be anything as long as it is a {@link String} and is used to resolve the scripts
 * at compilation time. For inline scripts, the key can be a description of the script. For stored and file scripts,
 * the source must match a key in the predefined set of scripts.
 *
 * The function is used to provide the result of the script execution and can return anything.
 */
public class MockScriptEngine implements ScriptEngine {

    public static final String NAME = "mockscript";

    private final String type;
    private final Map<String, Function<Map<String, Object>, Object>> scripts;

    public MockScriptEngine(String type, Map<String, Function<Map<String, Object>, Object>> scripts) {
        this.type = type;
        this.scripts = Collections.unmodifiableMap(scripts);
    }

    public MockScriptEngine() {
        this(NAME, Collections.emptyMap());
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
        // Scripts are always resolved using the script's source. For inline scripts, it's easy because they don't have names and the
        // source is always provided. For stored and file scripts, the source of the script must match the key of a predefined script.
        Function<Map<String, Object>, Object> script = scripts.get(source);
        if (script == null) {
            throw new IllegalArgumentException("No pre defined script matching [" + source + "] for script with name [" + name + "], " +
                    "did you declare the mocked script?");
        }
        MockCompiledScript mockCompiled = new MockCompiledScript(name, params, source, script);
        if (context.instanceClazz.equals(SearchScript.class)) {
            SearchScript.Factory factory = mockCompiled::createSearchScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ExecutableScript.class)) {
            ExecutableScript.Factory factory = mockCompiled::createExecutableScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(TemplateScript.class)) {
            TemplateScript.Factory factory = vars -> {
                // TODO: need a better way to implement all these new contexts
                // this is just a shim to act as an executable script just as before
                ExecutableScript execScript = mockCompiled.createExecutableScript(vars);
                    return new TemplateScript(vars) {
                        @Override
                        public String execute() {
                            return (String) execScript.run();
                        }
                    };
                };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(FilterScript.class)) {
            FilterScript.Factory factory = mockCompiled::createFilterScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(SimilarityScript.class)) {
            SimilarityScript.Factory factory = mockCompiled::createSimilarityScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(SimilarityWeightScript.class)) {
            SimilarityWeightScript.Factory factory = mockCompiled::createSimilarityWeightScript;
            return context.factoryClazz.cast(factory);
        }
        throw new IllegalArgumentException("mock script engine does not know how to handle context [" + context.name + "]");
    }

    public class MockCompiledScript {

        private final String name;
        private final String source;
        private final Map<String, String> options;
        private final Function<Map<String, Object>, Object> script;

        public MockCompiledScript(String name, Map<String, String> options, String source, Function<Map<String, Object>, Object> script) {
            this.name = name;
            this.source = source;
            this.options = options;
            this.script = script;
        }

        public String getName() {
            return name;
        }

        public ExecutableScript createExecutableScript(Map<String, Object> params) {
            Map<String, Object> context = new HashMap<>();
            if (options != null) {
                context.putAll(options); // TODO: remove this once scripts know to look for options under options key
                context.put("options", options);
            }
            if (params != null) {
                context.putAll(params); // TODO: remove this once scripts know to look for params under params key
                context.put("params", params);
            }
            return new MockExecutableScript(context, script != null ? script : ctx -> source);
        }

        public SearchScript.LeafFactory createSearchScript(Map<String, Object> params, SearchLookup lookup) {
            Map<String, Object> context = new HashMap<>();
            if (options != null) {
                context.putAll(options); // TODO: remove this once scripts know to look for options under options key
                context.put("options", options);
            }
            if (params != null) {
                context.putAll(params); // TODO: remove this once scripts know to look for params under params key
                context.put("params", params);
            }
            return new MockSearchScript(lookup, context, script != null ? script : ctx -> source);
        }


        public FilterScript.LeafFactory createFilterScript(Map<String, Object> params, SearchLookup lookup) {
            return new MockFilterScript(lookup, params, script);
        }

        public SimilarityScript createSimilarityScript() {
            return new MockSimilarityScript(script != null ? script : ctx -> 42d);
        }

        public SimilarityWeightScript createSimilarityWeightScript() {
            return new MockSimilarityWeightScript(script != null ? script : ctx -> 42d);
        }
    }

    public class MockExecutableScript implements ExecutableScript {

        private final Function<Map<String, Object>, Object> script;
        private final Map<String, Object> vars;

        public MockExecutableScript(Map<String, Object> vars, Function<Map<String, Object>, Object> script) {
            this.vars = vars;
            this.script = script;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

        @Override
        public Object run() {
            return script.apply(vars);
        }
    }

    public class MockSearchScript implements SearchScript.LeafFactory {

        private final Function<Map<String, Object>, Object> script;
        private final Map<String, Object> vars;
        private final SearchLookup lookup;

        public MockSearchScript(SearchLookup lookup, Map<String, Object> vars, Function<Map<String, Object>, Object> script) {
            this.lookup = lookup;
            this.vars = vars;
            this.script = script;
        }

        @Override
        public SearchScript newInstance(LeafReaderContext context) throws IOException {
            LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

            Map<String, Object> ctx = new HashMap<>(leafLookup.asMap());
            if (vars != null) {
                ctx.putAll(vars);
            }

            return new SearchScript(vars, lookup, context) {
                @Override
                public Object run() {
                    return script.apply(ctx);
                }

                @Override
                public long runAsLong() {
                    return ((Number) run()).longValue();
                }

                @Override
                public double runAsDouble() {
                    return ((Number) run()).doubleValue();
                }

                @Override
                public void setNextVar(String name, Object value) {
                    ctx.put(name, value);
                }

                @Override
                public void setScorer(Scorer scorer) {
                    ctx.put("_score", new ScoreAccessor(scorer));
                }

                @Override
                public void setDocument(int doc) {
                    leafLookup.setDocument(doc);
                }
            };
        }

        @Override
        public boolean needs_score() {
            return true;
        }
    }


    public static class MockFilterScript implements FilterScript.LeafFactory {

        private final Function<Map<String, Object>, Object> script;
        private final Map<String, Object> vars;
        private final SearchLookup lookup;

        public MockFilterScript(SearchLookup lookup, Map<String, Object> vars, Function<Map<String, Object>, Object> script) {
            this.lookup = lookup;
            this.vars = vars;
            this.script = script;
        }

        public FilterScript newInstance(LeafReaderContext context) throws IOException {
            LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
            Map<String, Object> ctx = new HashMap<>(leafLookup.asMap());
            if (vars != null) {
                ctx.putAll(vars);
            }
            return new FilterScript(ctx, lookup, context) {
                @Override
                public boolean execute() {
                    return (boolean) script.apply(ctx);
                }

                @Override
                public void setDocument(int doc) {
                    leafLookup.setDocument(doc);
                }
            };
        }
    }

    public class MockSimilarityScript extends SimilarityScript {

        private final Function<Map<String, Object>, Object> script;

        MockSimilarityScript(Function<Map<String, Object>, Object> script) {
            this.script = script;
        }

        @Override
        public double execute(double weight, Query query, Field field, Term term, Doc doc) throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("weight", weight);
            map.put("query", query);
            map.put("field", field);
            map.put("term", term);
            map.put("doc", doc);
            return ((Number) script.apply(map)).doubleValue();
        }
    }

    public class MockSimilarityWeightScript extends SimilarityWeightScript {

        private final Function<Map<String, Object>, Object> script;

        MockSimilarityWeightScript(Function<Map<String, Object>, Object> script) {
            this.script = script;
        }

        @Override
        public double execute(Query query, Field field, Term term) throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("query", query);
            map.put("field", field);
            map.put("term", term);
            return ((Number) script.apply(map)).doubleValue();
        }
    }

    public static Script mockInlineScript(final String script) {
        return new Script(ScriptType.INLINE, "mock", script, emptyMap());
    }

}
