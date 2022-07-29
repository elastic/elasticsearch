/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockDeterministicScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.xpack.core.monitoring.test.MockPainlessScriptEngine;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.condition.WatcherConditionScript;
import org.elasticsearch.xpack.watcher.transform.script.WatcherTransformScript;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A mock scripting engine that allows for scripts used in the watcher cluster alert definitions to be parsed
 * for the purposes of testing their creation and decommissioning. Their accuracy as watch definitions should
 * be verified via some other avenue.
 */
public class MockClusterAlertScriptEngine extends MockPainlessScriptEngine {

    /**
     * The plugin that creates this mock script engine. Overrides the original mock engine to inject this
     * implementation instead of the parent class.
     */
    public static class TestPlugin extends MockPainlessScriptEngine.TestPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new MockClusterAlertScriptEngine();
        }
    }

    @Override
    public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> options) {
        // Conditions - Default to false evaluation in testing
        if (context.instanceClazz.equals(WatcherConditionScript.class)) {
            return context.factoryClazz.cast(new MockWatcherConditionScript(MockDeterministicScript.asDeterministic(p -> false)));
        }

        // Transform - Return empty Map from the transform function
        if (context.instanceClazz.equals(WatcherTransformScript.class)) {
            return context.factoryClazz.cast(
                new MockWatcherTransformScript(MockDeterministicScript.asDeterministic(p -> new HashMap<String, Object>()))
            );
        }

        // We want to just add an allowance for watcher scripts, and to delegate everything else to the parent class.
        return super.compile(name, script, context, options);
    }

    /**
     * A mock watcher condition script that executes a given function instead of whatever the source provided was.
     */
    static class MockWatcherConditionScript implements WatcherConditionScript.Factory {

        private final MockDeterministicScript script;

        MockWatcherConditionScript(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public WatcherConditionScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext) {
            return new WatcherConditionScript(params, watcherContext) {
                @Override
                public boolean execute() {
                    return (boolean) script.apply(getParams());
                }
            };
        }
    }

    /**
     * A mock watcher transformation script that performs a given function instead of whatever the source provided was.
     */
    static class MockWatcherTransformScript implements WatcherTransformScript.Factory {

        private final MockDeterministicScript script;

        MockWatcherTransformScript(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public WatcherTransformScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext, Payload payload) {
            return new WatcherTransformScript(params, watcherContext, payload) {
                @Override
                public Object execute() {
                    return script.apply(getParams());
                }
            };
        }
    }
}
