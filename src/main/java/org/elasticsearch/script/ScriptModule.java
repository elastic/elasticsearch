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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;

import java.util.List;
import java.util.Map;

/**
 * An {@link org.elasticsearch.common.inject.Module} which manages {@link ScriptEngineService}s, as well
 * as named script
 */
public class ScriptModule extends AbstractModule {

    private final Settings settings;

    private final List<Class<? extends ScriptEngineService>> scriptEngines = Lists.newArrayList();

    private final Map<String, Class<? extends NativeScriptFactory>> scripts = Maps.newHashMap();

    private final List<ScriptContext.Plugin> customScriptContexts = Lists.newArrayList();

    public ScriptModule(Settings settings) {
        this.settings = settings;
    }

    public void addScriptEngine(Class<? extends ScriptEngineService> scriptEngine) {
        scriptEngines.add(scriptEngine);
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

    @Override
    protected void configure() {
        MapBinder<String, NativeScriptFactory> scriptsBinder
                = MapBinder.newMapBinder(binder(), String.class, NativeScriptFactory.class);
        for (Map.Entry<String, Class<? extends NativeScriptFactory>> entry : scripts.entrySet()) {
            scriptsBinder.addBinding(entry.getKey()).to(entry.getValue()).asEagerSingleton();
        }

        // now, check for config based ones
        Map<String, Settings> nativeSettings = settings.getGroups("script.native");
        for (Map.Entry<String, Settings> entry : nativeSettings.entrySet()) {
            String name = entry.getKey();
            Class<? extends NativeScriptFactory> type = entry.getValue().getAsClass("type", NativeScriptFactory.class);
            if (type == NativeScriptFactory.class) {
                throw new ElasticsearchIllegalArgumentException("type is missing for native script [" + name + "]");
            }
            scriptsBinder.addBinding(name).to(type).asEagerSingleton();
        }

        Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
        multibinder.addBinding().to(NativeScriptEngineService.class);

        try {
            settings.getClassLoader().loadClass("groovy.lang.GroovyClassLoader");
            multibinder.addBinding().to(GroovyScriptEngineService.class).asEagerSingleton();
        } catch (Throwable t) {
            Loggers.getLogger(ScriptService.class, settings).debug("failed to load groovy", t);
        }
        
        try {
            settings.getClassLoader().loadClass("com.github.mustachejava.Mustache");
            multibinder.addBinding().to(MustacheScriptEngineService.class).asEagerSingleton();
        } catch (Throwable t) {
            Loggers.getLogger(ScriptService.class, settings).debug("failed to load mustache", t);
        }

        try {
            settings.getClassLoader().loadClass("org.apache.lucene.expressions.Expression");
            multibinder.addBinding().to(ExpressionScriptEngineService.class).asEagerSingleton();
        } catch (Throwable t) {
            Loggers.getLogger(ScriptService.class, settings).debug("failed to load lucene expressions", t);
        }

        for (Class<? extends ScriptEngineService> scriptEngine : scriptEngines) {
            multibinder.addBinding().to(scriptEngine).asEagerSingleton();
        }

        bind(ScriptContextRegistry.class).toInstance(new ScriptContextRegistry(customScriptContexts));
        bind(ScriptService.class).asEagerSingleton();
    }
}
