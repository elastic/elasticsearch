/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * A script plugin that uses {@link MockScriptEngine} as the script engine for tests.
 */
public abstract class MockScriptPlugin extends Plugin implements ScriptPlugin {

    public static final String NAME = MockScriptEngine.NAME;

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new MockScriptEngine(pluginScriptLang(), pluginScripts(), nonDeterministicPluginScripts(), pluginContextCompilers());
    }

    protected abstract Map<String, Function<Map<String, Object>, Object>> pluginScripts();

    protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() { return Collections.emptyMap(); }

    protected Map<ScriptContext<?>, MockScriptEngine.ContextCompiler> pluginContextCompilers() {
        return Collections.emptyMap();
    }

    public String pluginScriptLang() {
        return NAME;
    }
}
