/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginsLoader;

import java.util.Map;
import java.util.stream.Collectors;

class PluginsResolver {
    private final Map<ModuleLayer, PluginDescriptor> modularPluginDescriptorByLayer;

    private PluginsResolver(Map<ModuleLayer, PluginDescriptor> modularPluginDescriptorByLayer) {
        this.modularPluginDescriptorByLayer = modularPluginDescriptorByLayer;
    }

    public static PluginsResolver create(PluginsLoader pluginsLoader) {
        var modularPluginDescriptorByLayer = pluginsLoader.pluginLayers()
            .filter(e -> e.pluginModuleLayer() != null && e.pluginModuleLayer() != ModuleLayer.boot())
            .collect(Collectors.toUnmodifiableMap(PluginsLoader.PluginLayer::pluginModuleLayer, e -> e.pluginBundle().pluginDescriptor()));

        return new PluginsResolver(modularPluginDescriptorByLayer);
    }

    public String resolveClassToPluginName(Class<?> clazz) {
        var module = clazz.getModule();
        var layer = module.getLayer();

        if (layer != null && layer != ModuleLayer.boot()) {
            // Potentially a modular plugin
            var pluginDescriptor = modularPluginDescriptorByLayer.get(layer);
            return pluginDescriptor == null ? null : pluginDescriptor.getName();
        } else if (module.isNamed() == false) {
            // Potentially a non-modular plugin
            // TODO: fallback to checking the clazz.getClassLoader() for non-modular plugins
            return null;
        }
        // Not a plugin
        return null;
    }
}
