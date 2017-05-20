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

package org.elasticsearch.script;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ScriptPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Manages building {@link ScriptService}.
 */
public class ScriptModule {
    private final ScriptService scriptService;

    /**
     * Build from {@linkplain ScriptPlugin}s. Convenient for normal use but not great for tests. See
     * {@link ScriptModule#ScriptModule(Settings, List, List)} for easier use in tests.
     */
    public static ScriptModule create(Settings settings, List<ScriptPlugin> scriptPlugins) {
        List<ScriptEngine> scriptEngines = scriptPlugins.stream().map(x -> x.getScriptEngine(settings))
            .filter(Objects::nonNull).collect(Collectors.toList());
        List<ScriptContext.Plugin> plugins = scriptPlugins.stream().map(x -> x.getCustomScriptContexts()).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new ScriptModule(settings, scriptEngines, plugins);
    }

    /**
     * Build {@linkplain ScriptEngine} and {@linkplain ScriptContext.Plugin}.
     */
    public ScriptModule(Settings settings, List<ScriptEngine> scriptEngines,
                        List<ScriptContext.Plugin> customScriptContexts) {
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        Map<String, ScriptEngine> enginesByName = getEnginesByName(scriptEngines);
        try {
            scriptService = new ScriptService(settings, enginesByName, scriptContextRegistry);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't setup ScriptService", e);
        }
    }

    private Map<String, ScriptEngine> getEnginesByName(List<ScriptEngine> engines) {
        Map<String, ScriptEngine> enginesByName = new HashMap<>();
        for (ScriptEngine engine : engines) {
            ScriptEngine existing = enginesByName.put(engine.getType(), engine);
            if (existing != null) {
                throw new IllegalArgumentException("scripting language [" + engine.getType() + "] defined for engine [" +
                    existing.getClass().getName() + "] and [" + engine.getClass().getName());
            }
        }
        return Collections.unmodifiableMap(enginesByName);
    }

    /**
     * Service responsible for managing scripts.
     */
    public ScriptService getScriptService() {
        return scriptService;
    }

    /**
     * Allow the script service to register any settings update handlers on the cluster settings
     */
    public void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }
}
