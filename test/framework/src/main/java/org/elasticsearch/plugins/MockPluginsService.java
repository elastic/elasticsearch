/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.spi.SPIClassIterator;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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
        super(settings, environment.configDir(), new PluginsLoader(Collections.emptySet(), Collections.emptySet(), Collections.emptyMap()));

        List<LoadedPlugin> pluginsLoaded = new ArrayList<>();

        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, environment.configDir());
            PluginDescriptor pluginInfo = new PluginDescriptor(
                pluginClass.getName(),
                "classpath plugin",
                "NA",
                Build.current().version(),
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
            pluginsLoaded.add(new LoadedPlugin(pluginInfo, plugin, MockPluginsService.class.getClassLoader()));
        }
        loadExtensions(pluginsLoaded);
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

    private static final Map<Class<?>, Collection<Class<?>>> spiClassesByService = ConcurrentCollections.newConcurrentMap();

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> List<? extends T> loadServiceProviders(Class<T> service) {
        // We use a map here to avoid duplicates because SPIClassIterator will match
        // all plugins in MockNode, because all plugins are loaded by the same class loader.
        // Each entry in the map is a unique service provider implementation.
        Map<Class<?>, T> result = new HashMap<>();
        for (LoadedPlugin pluginTuple : plugins()) {
            var plugin = pluginTuple.instance();
            var classLoader = plugin.getClass().getClassLoader();
            final Collection<? extends T> extension;
            if (classLoader == ClassLoader.getSystemClassLoader()) {
                // only determine the spi classes handled by the system classloader that loads most plugins in tests once to save test time;
                extension = createExtensions(service, plugin, (Iterator) spiClassesByService.computeIfAbsent(service, s -> {
                    var res = new ArrayList<Class<? extends T>>();
                    SPIClassIterator.get(service, classLoader).forEachRemaining(res::add);
                    return List.copyOf(res);
                }).iterator(), result::containsKey);
            } else {
                extension = createExtensions(service, plugin, result::containsKey);
            }
            extension.forEach(e -> result.put(e.getClass(), e));
        }

        return List.copyOf(result.values());
    }

    /**
     * When we load tests with MockNode, all plugins are loaded with the same class loader,
     * which breaks loading service providers with our SPIClassIterator. Since all plugins are
     * loaded in the same class loader, we find all plugins for any class found by the SPIClassIterator
     * causing us to pass plugin types to createExtension that aren't actually part of that plugin.
     * This modified createExtensions, checks for the type and returns an empty list if the
     * plugin class type is incompatible. It also skips loading extension types that have already
     * been loaded, so that duplicates are not created.
     */
    static <T> List<? extends T> createExtensions(
        Class<T> extensionPointType,
        Plugin plugin,
        Predicate<Class<? extends T>> loadedPredicate
    ) {
        Iterator<Class<? extends T>> classIterator = SPIClassIterator.get(extensionPointType, plugin.getClass().getClassLoader());
        return createExtensions(extensionPointType, plugin, classIterator, loadedPredicate);
    }

    private static <T> List<T> createExtensions(
        Class<T> extensionPointType,
        Plugin plugin,
        Iterator<Class<? extends T>> classIterator,
        Predicate<Class<? extends T>> loadedPredicate
    ) {
        List<T> extensions = new ArrayList<>();
        while (classIterator.hasNext()) {
            Class<? extends T> extensionClass = classIterator.next();
            if (loadedPredicate.test(extensionClass)) {
                // skip extensions that have already been loaded
                continue;
            }

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
