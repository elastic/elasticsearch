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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Manages building {@link ScriptService} and {@link ScriptSettings} from a list of plugins.
 */
public class ScriptModule {
    private final ScriptSettings scriptSettings;
    private final ScriptService scriptService;

    /**
     * Build from {@linkplain ScriptPlugin}s. Convenient for normal use but not great for tests. See
     * {@link ScriptModule#ScriptModule(Settings, Environment, ResourceWatcherService, List, List)} for easier use in tests.
     */
    public static ScriptModule create(Settings settings, Environment environment,
                                      ResourceWatcherService resourceWatcherService, List<ScriptPlugin> scriptPlugins) {
        Map<String, NativeScriptFactory> factoryMap = scriptPlugins.stream().flatMap(x -> x.getNativeScripts().stream())
            .collect(Collectors.toMap(NativeScriptFactory::getName, Function.identity()));
        NativeScriptEngineService nativeScriptEngineService = new NativeScriptEngineService(settings, factoryMap);
        List<ScriptEngineService> scriptEngineServices = scriptPlugins.stream().map(x -> x.getScriptEngineService(settings))
            .filter(Objects::nonNull).collect(Collectors.toList());
        scriptEngineServices.add(nativeScriptEngineService);
        List<ScriptContext.Plugin> plugins = scriptPlugins.stream().map(x -> x.getCustomScriptContexts()).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new ScriptModule(settings, environment, resourceWatcherService, scriptEngineServices, plugins);
    }

    /**
     * Build {@linkplain ScriptEngineService} and {@linkplain ScriptContext.Plugin}.
     */
    public ScriptModule(Settings settings, Environment environment,
                        ResourceWatcherService resourceWatcherService, List<ScriptEngineService> scriptEngineServices,
                        List<ScriptContext.Plugin> customScriptContexts) {
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngineServices);
        scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        try {
            scriptService = new ScriptService(settings, environment, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry,
                    scriptSettings);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't setup ScriptService", e);
        }
    }

    /**
     * Extra settings for scripts.
     */
    public List<Setting<?>> getSettings() {
        return scriptSettings.getSettings();
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
