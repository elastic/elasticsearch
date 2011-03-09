/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.script.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final AtomicLong counter = new AtomicLong();

    private final GroovyClassLoader loader;

    @Inject public GroovyScriptEngineService(Settings settings) {
        super(settings);
        this.loader = new GroovyClassLoader(settings.getClassLoader());
    }

    @Override public void close() {
        loader.clearCache();
    }

    @Override public String[] types() {
        return new String[]{"groovy"};
    }

    @Override public String[] extensions() {
        return new String[]{"groovy"};
    }

    @Override public Object compile(String script) {
        return loader.parseClass(script, generateScriptName());
    }

    @SuppressWarnings({"unchecked"})
    @Override public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        try {
            Class scriptClass = (Class) compiledScript;
            Script scriptObject = (Script) scriptClass.newInstance();
            Binding binding = new Binding();
            if (vars != null) {
                binding.getVariables().putAll(vars);
            }
            scriptObject.setBinding(binding);
            return new GroovyExecutableScript(scriptObject);
        } catch (Exception e) {
            throw new ScriptException("failed to build executable script", e);
        }
    }

    @Override public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        try {
            Class scriptClass = (Class) compiledScript;
            Script scriptObject = (Script) scriptClass.newInstance();
            Binding binding = new Binding();
            binding.getVariables().putAll(lookup.asMap());
            if (vars != null) {
                binding.getVariables().putAll(vars);
            }
            scriptObject.setBinding(binding);
            return new GroovySearchScript(scriptObject, lookup);
        } catch (Exception e) {
            throw new ScriptException("failed to build search script", e);
        }
    }

    @Override public Object execute(Object compiledScript, Map<String, Object> vars) {
        try {
            Class scriptClass = (Class) compiledScript;
            Script scriptObject = (Script) scriptClass.newInstance();
            Binding binding = new Binding(vars);
            scriptObject.setBinding(binding);
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException("failed to execute script", e);
        }
    }

    @Override public Object unwrap(Object value) {
        return value;
    }

    private String generateScriptName() {
        return "Script" + counter.incrementAndGet() + ".groovy";
    }

    public static class GroovyExecutableScript implements ExecutableScript {

        private final Script script;

        public GroovyExecutableScript(Script script) {
            this.script = script;
        }

        @Override public void setNextVar(String name, Object value) {
            script.getBinding().getVariables().put(name, value);
        }

        @Override public Object run() {
            return script.run();
        }

        @Override public Object unwrap(Object value) {
            return value;
        }
    }

    public static class GroovySearchScript implements SearchScript {

        private final Script script;

        private final SearchLookup lookup;

        public GroovySearchScript(Script script, SearchLookup lookup) {
            this.script = script;
            this.lookup = lookup;
        }

        @Override public void setScorer(Scorer scorer) {
            lookup.setScorer(scorer);
        }

        @Override public void setNextReader(IndexReader reader) {
            lookup.setNextReader(reader);
        }

        @Override public void setNextDocId(int doc) {
            lookup.setNextDocId(doc);
        }

        @Override public void setNextScore(float score) {
            script.getBinding().getVariables().put("_score", score);
        }

        @Override public void setNextVar(String name, Object value) {
            script.getBinding().getVariables().put(name, value);
        }

        @Override public Object run() {
            return script.run();
        }

        @Override public float runAsFloat() {
            return ((Number) run()).floatValue();
        }

        @Override public long runAsLong() {
            return ((Number) run()).longValue();
        }

        @Override public double runAsDouble() {
            return ((Number) run()).doubleValue();
        }

        @Override public Object unwrap(Object value) {
            return value;
        }
    }
}
