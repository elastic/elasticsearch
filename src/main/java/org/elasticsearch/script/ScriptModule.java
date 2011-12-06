/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.mvel.MvelScriptEngineService;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class ScriptModule extends AbstractModule {

    private final Settings settings;

    private final List<Class<? extends ScriptEngineService>> scriptEngines = Lists.newArrayList();

    private final Map<String, Class<? extends NativeScriptFactory>> scripts = Maps.newHashMap();

    public ScriptModule(Settings settings) {
        this.settings = settings;
    }

    public void addScriptEngine(Class<? extends ScriptEngineService> scriptEngine) {
        scriptEngines.add(scriptEngine);
    }

    public void registerScript(String name, Class<? extends NativeScriptFactory> script) {
        scripts.put(name, script);
    }

    @Override
    protected void configure() {
        MapBinder<String, NativeScriptFactory> scriptsBinder
                = MapBinder.newMapBinder(binder(), String.class, NativeScriptFactory.class);
        for (Map.Entry<String, Class<? extends NativeScriptFactory>> entry : scripts.entrySet()) {
            scriptsBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        // now, check for config based ones
        Map<String, Settings> nativeSettings = settings.getGroups("script.native");
        for (Map.Entry<String, Settings> entry : nativeSettings.entrySet()) {
            String name = entry.getKey();
            Class<? extends NativeScriptFactory> type = entry.getValue().getAsClass("type", NativeScriptFactory.class);
            if (type == NativeScriptFactory.class) {
                throw new ElasticSearchIllegalArgumentException("type is missing for native script [" + name + "]");
            }
            scriptsBinder.addBinding(name).to(type);
        }

        Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
        multibinder.addBinding().to(NativeScriptEngineService.class);
        try {
            multibinder.addBinding().to(MvelScriptEngineService.class);
        } catch (Throwable t) {
            // no MVEL
        }
        for (Class<? extends ScriptEngineService> scriptEngine : scriptEngines) {
            multibinder.addBinding().to(scriptEngine);
        }

        bind(ScriptService.class).asEagerSingleton();
    }
}
