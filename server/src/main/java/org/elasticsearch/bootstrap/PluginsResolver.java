/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.plugins.PluginsLoader;

import java.util.HashMap;
import java.util.Map;

class PluginsResolver {
    private final Map<Module, String> pluginNameByModule;

    private PluginsResolver(Map<Module, String> pluginNameByModule) {
        this.pluginNameByModule = pluginNameByModule;
    }

    public static PluginsResolver create(PluginsLoader pluginsLoader) {
        Map<Module, String> pluginNameByModule = new HashMap<>();

        pluginsLoader.pluginLayers().forEach(pluginLayer -> {
            var pluginName = pluginLayer.pluginBundle().pluginDescriptor().getName();
            if (pluginLayer.pluginModuleLayer() != null && pluginLayer.pluginModuleLayer() != ModuleLayer.boot()) {
                // This plugin is a Java Module
                for (var module : pluginLayer.pluginModuleLayer().modules()) {
                    pluginNameByModule.put(module, pluginName);
                }
            } else {
                // This plugin is not modularized
                pluginNameByModule.put(pluginLayer.pluginClassLoader().getUnnamedModule(), pluginName);
            }
        });

        return new PluginsResolver(pluginNameByModule);
    }

    public String resolveClassToPluginName(Class<?> clazz) {
        var module = clazz.getModule();
        return pluginNameByModule.get(module);
    }
}
