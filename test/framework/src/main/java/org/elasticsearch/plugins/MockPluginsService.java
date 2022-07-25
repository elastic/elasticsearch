/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MockPluginsService extends PluginsService {

    private static final Logger logger = LogManager.getLogger(MockPluginsService.class);

    private final List<LoadedPlugin> classpathPlugins;

    /**
     * Constructs a new PluginService
     *
     * @param settings         The settings of the system
     * @param environment      The environment for the plugin
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public MockPluginsService(Settings settings, Environment environment, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(settings, environment.configFile(), environment.modulesFile(), environment.pluginsFile());

        final Path configPath = environment.configFile();

        List<LoadedPlugin> pluginsLoaded = new ArrayList<>();

        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginDescriptor pluginInfo = new PluginDescriptor(
                pluginClass.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                Integer.toString(Runtime.version().feature()),
                pluginClass.getName(),
                null,
                Collections.emptyList(),
                false,
                PluginType.ISOLATED,
                "",
                false
            );
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new LoadedPlugin(pluginInfo, plugin));
        }

        this.classpathPlugins = List.copyOf(pluginsLoaded);
    }

    @Override
    protected final List<LoadedPlugin> plugins() {
        return this.classpathPlugins;
    }

    @Override
    public PluginsAndModules info() {
        return new PluginsAndModules(
            this.classpathPlugins.stream().map(LoadedPlugin::descriptor).map(PluginRuntimeInfo::new).toList(),
            List.of()
        );
    }
}
