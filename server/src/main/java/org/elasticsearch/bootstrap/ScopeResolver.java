/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.entitlement.runtime.policy.PolicyManager.PolicyScope;
import org.elasticsearch.plugins.PluginsLoader;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;

public class ScopeResolver {
    private final Map<Module, String> pluginNameByModule;

    /**
     * The package name containing classes from the APM agent.
     */
    private final String apmAgentPackageName;

    private ScopeResolver(Map<Module, String> pluginNameByModule, String apmAgentPackageName) {
        this.pluginNameByModule = pluginNameByModule;
        this.apmAgentPackageName = apmAgentPackageName;
    }

    public static ScopeResolver create(Stream<PluginsLoader.PluginLayer> pluginLayers, String apmAgentPackageName) {
        Map<Module, String> pluginNameByModule = new HashMap<>();

        pluginLayers.forEach(pluginLayer -> {
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

        return new ScopeResolver(pluginNameByModule, apmAgentPackageName);
    }

    public PolicyScope resolveClassToScope(Class<?> clazz) {
        var module = clazz.getModule();
        var scopeName = getScopeName(module);
        if (isServerModule(module)) {
            return PolicyScope.server(scopeName);
        }
        String pluginName = pluginNameByModule.get(module);
        if (pluginName != null) {
            return PolicyScope.plugin(pluginName, scopeName);
        }
        if (module.isNamed() == false && clazz.getPackageName().startsWith(apmAgentPackageName)) {
            // The APM agent is the only thing running non-modular in the system classloader
            return PolicyScope.apmAgent(ALL_UNNAMED);
        }
        return PolicyScope.unknown(scopeName);
    }

    private static boolean isServerModule(Module requestingModule) {
        return requestingModule.isNamed() && requestingModule.getLayer() == ModuleLayer.boot();
    }

    public static String getScopeName(Module requestingModule) {
        if (requestingModule.isNamed() == false) {
            return ALL_UNNAMED;
        } else {
            return requestingModule.getName();
        }
    }
}
