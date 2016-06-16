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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ScriptPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@link org.elasticsearch.common.inject.Module} which manages {@link ScriptEngineService}s, as well
 * as named script
 */
public class ScriptModule extends AbstractModule {

    protected final ScriptContextRegistry scriptContextRegistry;
    protected final ScriptEngineRegistry scriptEngineRegistry;
    protected final ScriptSettings scriptSettings;

    public ScriptModule(ScriptEngineService... services) {
        this(Arrays.asList(services), Collections.emptyList());
    }

    public ScriptModule(List<ScriptEngineService> scriptEngineServices,
                        List<ScriptContext.Plugin> customScriptContexts) {
        this.scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        this.scriptEngineRegistry = new ScriptEngineRegistry(scriptEngineServices);
        this.scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
    }

    /**
     * This method is called after all modules have been processed but before we actually validate all settings. This allows the
     * script extensions to add all their settings.
     */
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        scriptSettings.getScriptTypeSettings().forEach(settings::add);
        scriptSettings.getScriptContextSettings().forEach(settings::add);
        scriptSettings.getScriptLanguageSettings().forEach(settings::add);
        settings.add(scriptSettings.getDefaultScriptLanguageSetting());
        return settings;
    }


    @Override
    protected void configure() {
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        bind(ScriptContextRegistry.class).toInstance(scriptContextRegistry);
        bind(ScriptEngineRegistry.class).toInstance(scriptEngineRegistry);
        bind(ScriptSettings.class).toInstance(scriptSettings);
        bind(ScriptService.class).asEagerSingleton();
    }

    public static ScriptModule create(Settings settings, List<ScriptPlugin> scriptPlugins) {
        Map<String, NativeScriptFactory> factoryMap = scriptPlugins.stream().flatMap(x -> x.getNativeScripts().stream())
            .collect(Collectors.toMap(NativeScriptFactory::getName, Function.identity()));
        NativeScriptEngineService nativeScriptEngineService = new NativeScriptEngineService(settings, factoryMap);
        List<ScriptEngineService> scriptEngineServices = scriptPlugins.stream().map(x -> x.getScriptEngineService(settings))
            .filter(Objects::nonNull).collect(Collectors.toList());
        scriptEngineServices.add(nativeScriptEngineService);
        return new ScriptModule(scriptEngineServices, scriptPlugins.stream().map(x -> x.getCustomScriptContexts())
            .filter(Objects::nonNull).collect(Collectors.toList()));
    }
}
