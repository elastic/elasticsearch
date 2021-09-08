/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.script;

import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * A mock script engine that registers itself under the 'mustache' name so that
 * TextTemplateEngine (watcher)
 * uses it and adds validation that watcher tests don't rely on mustache templating/
 */
public class MockMustacheScriptEngine extends MockScriptEngine {

    public static final String NAME = "mustache";

    public static class TestPlugin extends MockScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new MockMustacheScriptEngine();
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.emptyMap();
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> params) {
        if (script.contains("{{") && script.contains("}}")) {
            throw new IllegalArgumentException("Fix your test to not rely on mustache");
        }
        if (context.instanceClazz.equals(TemplateScript.class) == false) {
            throw new IllegalArgumentException("mock mustache only understands template scripts, not [" + context.name + "]");
        }
        return context.factoryClazz.cast((TemplateScript.Factory) vars ->
            new TemplateScript(vars) {
                @Override
                public String execute() {
                    return script;
                }
            });
    }
}
