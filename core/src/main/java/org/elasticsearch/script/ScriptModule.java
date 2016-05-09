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
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.script.ScriptMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link org.elasticsearch.common.inject.Module} which manages {@link ScriptEngineService}s, as well
 * as named script
 */
public class ScriptModule extends AbstractModule {

    private final List<ScriptEngineRegistry.ScriptEngineRegistration> scriptEngineRegistrations = new ArrayList<>();

    {
        scriptEngineRegistrations.add(new ScriptEngineRegistry.ScriptEngineRegistration(NativeScriptEngineService.class,
                        NativeScriptEngineService.TYPES, ScriptMode.ON));
    }

    private final Map<String, Class<? extends NativeScriptFactory>> scripts = new HashMap<>();

    private final List<ScriptContext.Plugin> customScriptContexts = new ArrayList<>();


    public void addScriptEngine(ScriptEngineRegistry.ScriptEngineRegistration scriptEngineRegistration) {
        Objects.requireNonNull(scriptEngineRegistration);
        scriptEngineRegistrations.add(scriptEngineRegistration);
    }

    public void registerScript(String name, Class<? extends NativeScriptFactory> script) {
        scripts.put(name, script);
    }

    /**
     * Registers a custom script context that can be used by plugins to categorize the different operations that they use scripts for.
     * Fine-grained settings allow to enable/disable scripts per context.
     */
    public void registerScriptContext(ScriptContext.Plugin scriptContext) {
        customScriptContexts.add(scriptContext);
    }

    /**
     * This method is called after all modules have been processed but before we actually validate all settings. This allows the
     * script extensions to add all their settings.
     */
    public void prepareSettings(SettingsModule settingsModule) {
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngineRegistrations);
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);

        scriptSettings.getScriptTypeSettings().forEach(settingsModule::registerSetting);
        scriptSettings.getScriptContextSettings().forEach(settingsModule::registerSetting);
        scriptSettings.getScriptLanguageSettings().forEach(settingsModule::registerSetting);
        settingsModule.registerSetting(scriptSettings.getDefaultScriptLanguageSetting());
    }

    @Override
    protected void configure() {
        MapBinder<String, NativeScriptFactory> scriptsBinder
                = MapBinder.newMapBinder(binder(), String.class, NativeScriptFactory.class);
        for (Map.Entry<String, Class<? extends NativeScriptFactory>> entry : scripts.entrySet()) {
            scriptsBinder.addBinding(entry.getKey()).to(entry.getValue()).asEagerSingleton();
        }

        Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
        multibinder.addBinding().to(NativeScriptEngineService.class);

        for (ScriptEngineRegistry.ScriptEngineRegistration scriptEngineRegistration : scriptEngineRegistrations) {
            multibinder.addBinding().to(scriptEngineRegistration.getScriptEngineService()).asEagerSingleton();
        }


        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customScriptContexts);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngineRegistrations);
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);

        bind(ScriptContextRegistry.class).toInstance(scriptContextRegistry);
        bind(ScriptEngineRegistry.class).toInstance(scriptEngineRegistry);
        bind(ScriptSettings.class).toInstance(scriptSettings);
        bind(ScriptService.class).asEagerSingleton();
    }
}
