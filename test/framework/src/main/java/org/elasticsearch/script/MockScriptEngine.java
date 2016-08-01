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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

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
public class MockScriptEngine implements ScriptEngineService {

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
    public String getExtension() {
        return getType();
    }

    @Override
    public Object compile(String name, String source, Map<String, String> params) {
        // Scripts are always resolved using the script's source. For inline scripts, it's easy because they don't have names and the
        // source is always provided. For stored and file scripts, the source of the script must match the key of a predefined script.
        Function<Map<String, Object>, Object> script = scripts.get(source);
        if (script == null) {
            throw new IllegalArgumentException("No pre defined script matching [" + source + "] for script with name [" + name + "], " +
                    "did you declare the mocked script?");
        }
        return new MockCompiledScript(name, params, source, script);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        MockCompiledScript compiled = (MockCompiledScript) compiledScript.compiled();
        return compiled.createExecutableScript(vars);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        MockCompiledScript compiled = (MockCompiledScript) compiledScript.compiled();
        return compiled.createSearchScript(vars, lookup);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }


    public class MockCompiledScript {

        private final String name;
        private final String source;
        private final Map<String, String> params;
        private final Function<Map<String, Object>, Object> script;

        public MockCompiledScript(String name, Map<String, String> params, String source, Function<Map<String, Object>, Object> script) {
            this.name = name;
            this.source = source;
            this.params = params;
            this.script = script;
        }

        public String getName() {
            return name;
        }

        public ExecutableScript createExecutableScript(Map<String, Object> vars) {
            Map<String, Object> context = new HashMap<>();
            if (params != null) {
                context.putAll(params);
            }
            if (vars != null) {
                context.putAll(vars);
            }
            return new MockExecutableScript(context, script != null ? script : ctx -> new BytesArray(source));
        }

        public SearchScript createSearchScript(Map<String, Object> vars, SearchLookup lookup) {
            Map<String, Object> context = new HashMap<>();
            if (params != null) {
                context.putAll(params);
            }
            if (vars != null) {
                context.putAll(vars);
            }
            return new MockSearchScript(lookup, context, script != null ? script : ctx -> source);
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

    public class MockSearchScript implements SearchScript {

        private final Function<Map<String, Object>, Object> script;
        private final Map<String, Object> vars;
        private final SearchLookup lookup;

        public MockSearchScript(SearchLookup lookup, Map<String, Object> vars, Function<Map<String, Object>, Object> script) {
            this.lookup = lookup;
            this.vars = vars;
            this.script = script;
        }

        @Override
        public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
            LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

            Map<String, Object> ctx = new HashMap<>();
            ctx.putAll(leafLookup.asMap());
            if (vars != null) {
                ctx.putAll(vars);
            }

            AbstractSearchScript leafSearchScript = new AbstractSearchScript() {

                @Override
                public Object run() {
                    return script.apply(ctx);
                }

                @Override
                public void setNextVar(String name, Object value) {
                    ctx.put(name, value);
                }

                @Override
                public void setScorer(Scorer scorer) {
                    super.setScorer(scorer);
                    ctx.put("_score", new ScoreAccessor(scorer));
                }
            };
            leafSearchScript.setLookup(leafLookup);
            return leafSearchScript;
        }

        @Override
        public boolean needsScores() {
            return true;
        }
    }
}
