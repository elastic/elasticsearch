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
import org.elasticsearch.plugins.spi.SPIClassIterator;

import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                false,
                false,
                false
            );
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new LoadedPlugin(pluginInfo, plugin, pluginClass.getClassLoader(), ModuleLayer.boot()));
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

    @Override
    public <T> List<? extends T> loadServiceProviders(Class<T> service) {
        // We use a set here to avoid duplicates because SPIClassIterator will match
        // all plugins in MockNode, because all plugins are loaded by the same class loader.
        Set<T> result = new HashSet<>();

        for (LoadedPlugin pluginTuple : plugins()) {
            result.addAll(createExtensions(service, pluginTuple.instance()));
        }

        return List.copyOf(result);
    }

    /**
     * When we load tests with MockNode, all plugins are loaded with the same class loader,
     * which breaks loading service providers with our SPIClassIterator. Since all plugins are
     * loaded in the same class loader, we find all plugins for any class found by the SPIClassIterator
     * causing us to pass wrong plugin type to createExtension. This modified createExtensions, checks for
     * the type and returns an empty list if the plugin class type is incompatible.
     */
    static <T> List<? extends T> createExtensions(Class<T> extensionPointType, Plugin plugin) {
        SPIClassIterator<T> classIterator = SPIClassIterator.get(extensionPointType, plugin.getClass().getClassLoader());
        List<T> extensions = new ArrayList<>();
        while (classIterator.hasNext()) {
            Class<? extends T> extensionClass = classIterator.next();

            @SuppressWarnings("unchecked")
            Constructor<T>[] constructors = (Constructor<T>[]) extensionClass.getConstructors();
            boolean compatible = true;

            // We only check if we have incompatible one argument constructor, otherwise we let the code
            // fall-through to the PluginsService method that will check if we have valid service provider.
            // For one argument constructors we cannot validate from which plugin they should be loaded, which
            // is why we de-dup the instances by using a Set in loadServiceProviders.
            for (var constructor : constructors) {
                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0] != plugin.getClass()) {
                    compatible = false;
                    break;
                }
            }
            if (compatible) {
                extensions.add(createExtension(extensionClass, extensionPointType, plugin));
            }
        }
        return extensions;
    }
}
