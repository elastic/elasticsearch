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

package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.condition.WatcherConditionScript;
import org.elasticsearch.xpack.watcher.transform.script.WatcherTransformScript;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides a mock script engine with mock versions of watcher scripts.
 */
public abstract class WatcherMockScriptPlugin extends MockScriptPlugin {
    public static final Map<ScriptContext<?>, MockScriptEngine.ContextCompiler> CONTEXT_COMPILERS;
    static {
        Map<ScriptContext<?>, MockScriptEngine.ContextCompiler> compilers = new HashMap<>();
        compilers.put(WatcherConditionScript.CONTEXT, (script, options) ->
            (WatcherConditionScript.Factory) (params, watcherContext) ->
                new WatcherConditionScript(params, watcherContext) {
                    @Override
                    public boolean execute() {
                        Map<String, Object> vars = new HashMap<>();
                        vars.put("params", getParams());
                        vars.put("ctx", getCtx());
                        return (boolean) script.apply(vars);
                    }
                });
        compilers.put(WatcherTransformScript.CONTEXT, (script, options) ->
            (WatcherTransformScript.Factory) (params, watcherContext, payload) ->
                new WatcherTransformScript(params, watcherContext, payload) {
                    @Override
                    public Object execute() {
                        Map<String, Object> vars = new HashMap<>();
                        vars.put("params", getParams());
                        vars.put("ctx", getCtx());
                        return script.apply(vars);
                    }
                });
        CONTEXT_COMPILERS = Collections.unmodifiableMap(compilers);
    }

    public static final List<ScriptContext<?>> CONTEXTS = Collections.unmodifiableList(Arrays.asList(
        WatcherConditionScript.CONTEXT, WatcherTransformScript.CONTEXT, Watcher.SCRIPT_TEMPLATE_CONTEXT, Watcher.SCRIPT_SEARCH_CONTEXT
    ));

    @Override
    protected Map<ScriptContext<?>, MockScriptEngine.ContextCompiler> pluginContextCompilers() {
        return CONTEXT_COMPILERS;
    }

    public static ScriptService newMockScriptService(Map<String, Function<Map<String, Object>, Object>> scripts) {
        Map<String, ScriptEngine> engines = new HashMap<>();
        engines.put(MockScriptEngine.NAME,
            new MockScriptEngine(MockScriptEngine.NAME, scripts, CONTEXT_COMPILERS));
        Map<String, ScriptContext<?>> contexts = CONTEXTS.stream().collect(Collectors.toMap(o -> o.name, Function.identity()));
        return new ScriptService(Settings.EMPTY, engines, contexts);
    }
}
