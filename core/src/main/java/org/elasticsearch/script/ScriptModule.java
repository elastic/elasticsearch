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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Manages building {@link ScriptService} and {@link ScriptSettings} from a list of plugins.
 */
public class ScriptModule {
    /**
     * Build from {@linkplain ScriptPlugin}s. Convenient for normal use but not great for tests. See
     * {@link ScriptModule#ScriptModule(Settings, Environment, ResourceWatcherService, List, List, TemplateService.Backend)}
     * for easier use in tests.
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
        List<TemplateService.Backend> templateBackends = scriptPlugins.stream().map(x -> x.getTemplateBackend())
                .filter(Objects::nonNull).collect(Collectors.toList());
        TemplateService.Backend templateBackend;
        switch (templateBackends.size()) {
        case 0:
            templateBackend = null;
            break;
        case 1:
            templateBackend = templateBackends.get(0);
            break;
        default:
            throw new IllegalArgumentException("Elasticsearch only supports a single template backend but was started with ["
                    + templateBackends + "]");
        }
        return new ScriptModule(settings, environment, resourceWatcherService, scriptEngineServices, plugins, templateBackend);
    }

    private final ScriptSettings scriptSettings;
    private final ScriptService scriptService;
    private final TemplateService templateService;

    /**
     * Build {@linkplain ScriptEngineService} and {@linkplain ScriptContext.Plugin}.
     */
    public ScriptModule(Settings settings, Environment environment,
                        ResourceWatcherService resourceWatcherService, List<ScriptEngineService> scriptEngineServices,
                        List<ScriptContext.Plugin> customScriptContexts, @Nullable TemplateService.Backend templateBackend) {
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngineServices);
        ScriptMetrics scriptMetrics = new ScriptMetrics();
        // Note that if templateBackend is null this won't register any settings for it
        scriptSettings = new ScriptSettings(scriptEngineRegistry, templateBackend, scriptContextRegistry);

        try {
            scriptService = new ScriptService(settings, environment, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry,
                    scriptSettings, scriptMetrics);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't setup ScriptService", e);
        }

        if (templateBackend == null) {
            templateBackend = new TemplatesUnsupportedBackend();
        }
        try {
            templateService = new TemplateService(settings, environment, resourceWatcherService, templateBackend,
                    scriptContextRegistry, scriptSettings, scriptMetrics);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't setup TemplateService", e);
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
     * The service responsible for managing templates.
     */
    public TemplateService getTemplateService() {
        return templateService;
    }

    /**
     * Allow the script service to register any settings update handlers on the cluster settings
     */
    public void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }

    private static class TemplatesUnsupportedBackend implements TemplateService.Backend {
        @Override
        public String getType() {
            throw new UnsupportedOperationException("no template backend installed");
        }

        @Override
        public String getExtension() {
            throw new UnsupportedOperationException("no template backend installed");
        }

        @Override
        public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
            throw new UnsupportedOperationException("no template backend installed");
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript,
                Map<String, Object> vars) {
            throw new UnsupportedOperationException("no template backend installed");
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup,
                Map<String, Object> vars) {
            throw new UnsupportedOperationException("no template backend installed");
        }

        @Override
        public void close() throws IOException {
        }
    }
}
