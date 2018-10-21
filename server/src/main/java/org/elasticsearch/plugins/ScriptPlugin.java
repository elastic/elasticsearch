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
