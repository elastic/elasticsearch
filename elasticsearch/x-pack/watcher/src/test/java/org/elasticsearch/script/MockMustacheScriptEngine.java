/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.common.text.DefaultTextTemplateEngine;

import java.util.Map;

/**
 * A mock script engine that registers itself under the 'mustache' name so that
 * {@link DefaultTextTemplateEngine}
 * uses it and adds validation that watcher tests don't rely on mustache templating/
 */
public class MockMustacheScriptEngine extends MockScriptEngine {

    public static final String NAME = "mustache";

    public static class TestPlugin extends MockScriptEngine.TestPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new MockMustacheScriptEngine();
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public Object compile(String name, String script, Map<String, String> params) {
        if (script.contains("{{") && script.contains("}}")) {
            throw new IllegalArgumentException("Fix your test to not rely on mustache");
        }

        return super.compile(name, script, params);
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }
}
