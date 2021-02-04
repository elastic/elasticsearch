/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        CONTEXT_COMPILERS = Map.of(WatcherConditionScript.CONTEXT, (script, options) ->
                (WatcherConditionScript.Factory) (params, watcherContext) ->
                        new WatcherConditionScript(params, watcherContext) {
                            @Override
                            public boolean execute() {
                                Map<String, Object> vars = new HashMap<>();
                                vars.put("params", getParams());
                                vars.put("ctx", getCtx());
                                return (boolean) script.apply(vars);
                            }
                        }, WatcherTransformScript.CONTEXT, (script, options) ->
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
    }

    public static final List<ScriptContext<?>> CONTEXTS =
            List.of(WatcherConditionScript.CONTEXT, WatcherTransformScript.CONTEXT, Watcher.SCRIPT_TEMPLATE_CONTEXT);

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
