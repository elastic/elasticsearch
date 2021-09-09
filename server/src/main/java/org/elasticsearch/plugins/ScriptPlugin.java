/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugins;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

/**
 * An additional extension point for {@link Plugin}s that extends Elasticsearch's scripting functionality.
 */
public interface ScriptPlugin {

    /**
     * Returns a {@link ScriptEngine} instance or <code>null</code> if this plugin doesn't add a new script engine.
     * @param settings Node settings
     * @param contexts The contexts that {@link ScriptEngine#compile(String, String, ScriptContext, Map)} may be called with
     */
    default ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return null;
    }

    /**
     * Return script contexts this plugin wants to allow using.
     */
    default List<ScriptContext<?>> getContexts() {
        return Collections.emptyList();
    }
}
