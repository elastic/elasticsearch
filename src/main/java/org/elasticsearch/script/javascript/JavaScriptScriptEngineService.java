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

package org.elasticsearch.script.javascript;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScoreAccessor;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.javascript.support.NativeList;
import org.elasticsearch.script.javascript.support.NativeMap;
import org.elasticsearch.script.javascript.support.ScriptValueConverter;
import org.elasticsearch.search.lookup.SearchLookup;
import org.mozilla.javascript.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class JavaScriptScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final AtomicLong counter = new AtomicLong();

    private static WrapFactory wrapFactory = new CustomWrapFactory();

    private final int optimizationLevel;

    private Scriptable globalScope;

    @Inject
    public JavaScriptScriptEngineService(Settings settings) {
        super(settings);

        this.optimizationLevel = componentSettings.getAsInt("optimization_level", 1);

        Context ctx = Context.enter();
        try {
            ctx.setWrapFactory(wrapFactory);
            globalScope = ctx.initStandardObjects(null, true);
        } finally {
            Context.exit();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public String[] types() {
        return new String[]{"js", "javascript"};
    }

    @Override
    public String[] extensions() {
        return new String[]{"js"};
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        Context ctx = Context.enter();
        try {
            ctx.setWrapFactory(wrapFactory);
            ctx.setOptimizationLevel(optimizationLevel);
            return ctx.compileString(script, generateScriptName(), 1, null);
        } finally {
            Context.exit();
        }
    }

    @Override
    public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        Context ctx = Context.enter();
        try {
            ctx.setWrapFactory(wrapFactory);

            Scriptable scope = ctx.newObject(globalScope);
            scope.setPrototype(globalScope);
            scope.setParentScope(null);
            for (Map.Entry<String, Object> entry : vars.entrySet()) {
                ScriptableObject.putProperty(scope, entry.getKey(), entry.getValue());
            }

            return new JavaScriptExecutableScript((Script) compiledScript, scope);
        } finally {
            Context.exit();
        }
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        Context ctx = Context.enter();
        try {
            ctx.setWrapFactory(wrapFactory);

            Scriptable scope = ctx.newObject(globalScope);
            scope.setPrototype(globalScope);
            scope.setParentScope(null);

            for (Map.Entry<String, Object> entry : lookup.asMap().entrySet()) {
                ScriptableObject.putProperty(scope, entry.getKey(), entry.getValue());
            }

            if (vars != null) {
                for (Map.Entry<String, Object> entry : vars.entrySet()) {
                    ScriptableObject.putProperty(scope, entry.getKey(), entry.getValue());
                }
            }

            return new JavaScriptSearchScript((Script) compiledScript, scope, lookup);
        } finally {
            Context.exit();
        }
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        Context ctx = Context.enter();
        ctx.setWrapFactory(wrapFactory);
        try {
            Script script = (Script) compiledScript;
            Scriptable scope = ctx.newObject(globalScope);
            scope.setPrototype(globalScope);
            scope.setParentScope(null);

            for (Map.Entry<String, Object> entry : vars.entrySet()) {
                ScriptableObject.putProperty(scope, entry.getKey(), entry.getValue());
            }
            Object ret = script.exec(ctx, scope);
            return ScriptValueConverter.unwrapValue(ret);
        } finally {
            Context.exit();
        }
    }

    @Override
    public Object unwrap(Object value) {
        return ScriptValueConverter.unwrapValue(value);
    }

    private String generateScriptName() {
        return "Script" + counter.incrementAndGet() + ".js";
    }

    public static class JavaScriptExecutableScript implements ExecutableScript {

        private final Script script;

        private final Scriptable scope;

        public JavaScriptExecutableScript(Script script, Scriptable scope) {
            this.script = script;
            this.scope = scope;
        }

        @Override
        public Object run() {
            Context ctx = Context.enter();
            try {
                ctx.setWrapFactory(wrapFactory);
                return ScriptValueConverter.unwrapValue(script.exec(ctx, scope));
            } finally {
                Context.exit();
            }
        }

        @Override
        public void setNextVar(String name, Object value) {
            ScriptableObject.putProperty(scope, name, value);
        }

        @Override
        public Object unwrap(Object value) {
            return ScriptValueConverter.unwrapValue(value);
        }
    }

    public static class JavaScriptSearchScript implements SearchScript {

        private final Script script;

        private final Scriptable scope;

        private final SearchLookup lookup;

        public JavaScriptSearchScript(Script script, Scriptable scope, SearchLookup lookup) {
            this.script = script;
            this.scope = scope;
            this.lookup = lookup;
        }

        @Override
        public void setScorer(Scorer scorer) {
            Context ctx = Context.enter();
            ScriptableObject.putProperty(scope, "_score", wrapFactory.wrapAsJavaObject(ctx, scope, new ScoreAccessor(scorer), ScoreAccessor.class));
            Context.exit();
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            lookup.setNextReader(context);
        }

        @Override
        public void setNextDocId(int doc) {
            lookup.setNextDocId(doc);
        }

        @Override
        public void setNextVar(String name, Object value) {
            ScriptableObject.putProperty(scope, name, value);
        }

        @Override
        public void setNextSource(Map<String, Object> source) {
            lookup.source().setNextSource(source);
        }

        @Override
        public Object run() {
            Context ctx = Context.enter();
            try {
                ctx.setWrapFactory(wrapFactory);
                return ScriptValueConverter.unwrapValue(script.exec(ctx, scope));
            } finally {
                Context.exit();
            }
        }

        @Override
        public float runAsFloat() {
            return ((Number) run()).floatValue();
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
        public Object unwrap(Object value) {
            return ScriptValueConverter.unwrapValue(value);
        }
    }

    /**
     * Wrap Factory for Rhino Script Engine
     */
    public static class CustomWrapFactory extends WrapFactory {

        public CustomWrapFactory() {
            setJavaPrimitiveWrap(false); // RingoJS does that..., claims its annoying...
        }

        public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, Object javaObject, Class staticType) {
            if (javaObject instanceof Map) {
                return new NativeMap(scope, (Map) javaObject);
            }
            if (javaObject instanceof List) {
                return new NativeList(scope, (List) javaObject);
            }
            return super.wrapAsJavaObject(cx, scope, javaObject, staticType);
        }
    }
}
