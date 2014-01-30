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

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.CloseableIndexComponent;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * An internal wrapper around a {@link Plugin} instance that is associated with its {@code PluginInfo}.
 */
public class JvmPlugin implements Plugin {

    private final Plugin plugin;
    private final PluginInfo info;
    private final List<OnModuleReference> onModuleReferences;
    private final ESLogger logger;

    public JvmPlugin(Plugin plugin, PluginInfo info, ESLogger logger) {
        this.plugin = plugin;
        this.info = info;
        this.logger = logger;
        this.onModuleReferences = loadOnModuleReferences(plugin, logger);
    }

    public PluginInfo info() {
        return info;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public String description() {
        return plugin.description();
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return plugin.modules();
    }

    @Override
    public Collection<? extends Module> modules(Settings settings) {
        return plugin.modules(settings);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return plugin.services();
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        return plugin.indexModules();
    }

    @Override
    public Collection<? extends Module> indexModules(Settings settings) {
        return plugin.indexModules(settings);
    }

    @Override
    public Collection<Class<? extends CloseableIndexComponent>> indexServices() {
        return plugin.indexServices();
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        return plugin.shardModules();
    }

    @Override
    public Collection<? extends Module> shardModules(Settings settings) {
        return plugin.shardModules(settings);
    }

    @Override
    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        return plugin.shardServices();
    }

    @Override
    public Settings additionalSettings() {
        return plugin.additionalSettings();
    }

    public void processModule(Module module) {
        plugin.processModule(module);
        // see if there are onModule references
        for (OnModuleReference reference : onModuleReferences) {
            if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                try {
                    reference.onModuleMethod.invoke(plugin, module);
                } catch (Exception e) {
                    logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.name());
                }
            }
        }
    }

    private static List<OnModuleReference> loadOnModuleReferences(Plugin plugin, ESLogger logger) {
        List<OnModuleReference> refs = Lists.newArrayList();
        for (Method method : plugin.getClass().getDeclaredMethods()) {
            if (!method.getName().equals("onModule")) {
                continue;
            }
            if (method.getParameterTypes().length == 0 || method.getParameterTypes().length > 1) {
                logger.warn("Plugin: {} implementing onModule with no parameters or more than one parameter", plugin.name());
                continue;
            }
            Class moduleClass = method.getParameterTypes()[0];
            if (!Module.class.isAssignableFrom(moduleClass)) {
                logger.warn("Plugin: {} implementing onModule by the type is not of Module type {}", plugin.name(), moduleClass);
                continue;
            }
            method.setAccessible(true);
            refs.add(new OnModuleReference(moduleClass, method));
        }
        return refs;
    }

    static class OnModuleReference {
        public final Class<? extends Module> moduleClass;
        public final Method onModuleMethod;

        OnModuleReference(Class<? extends Module> moduleClass, Method onModuleMethod) {
            this.moduleClass = moduleClass;
            this.onModuleMethod = onModuleMethod;
        }
    }
}
