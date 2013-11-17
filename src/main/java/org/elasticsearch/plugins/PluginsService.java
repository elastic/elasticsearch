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

package org.elasticsearch.plugins;

import com.google.common.collect.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.CloseableIndexComponent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class PluginsService extends AbstractComponent {
    private static final String ES_PLUGIN_PROPERTIES = "es-plugin.properties";

    private final Environment environment;

    private final ImmutableMap<String, Plugin> plugins;

    private final ImmutableMap<Plugin, List<OnModuleReference>> onModuleReferences;

    private PluginsInfo cachedPluginsInfo;
    private final TimeValue refreshInterval;
    private long lastRefresh;

    static class OnModuleReference {
        public final Class<? extends Module> moduleClass;
        public final Method onModuleMethod;

        OnModuleReference(Class<? extends Module> moduleClass, Method onModuleMethod) {
            this.moduleClass = moduleClass;
            this.onModuleMethod = onModuleMethod;
        }
    }

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system
     * @param environment The environment of the system
     */
    public PluginsService(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;

        Map<String, Plugin> plugins = Maps.newHashMap();

        //first we load all the default plugins from the settings
        String[] defaultPluginsClasses = settings.getAsArray("plugin.types");
        for (String pluginClass : defaultPluginsClasses) {
            Plugin plugin = loadPlugin(pluginClass, settings);
            plugins.put(plugin.name(), plugin);
        }

        // now, find all the ones that are in the classpath
        loadPluginsIntoClassLoader();
        plugins.putAll(loadPluginsFromClasspath(settings));
        Set<String> sitePlugins = PluginsHelper.sitePlugins(this.environment);

        String[] mandatoryPlugins = settings.getAsArray("plugin.mandatory", null);
        if (mandatoryPlugins != null) {
            Set<String> missingPlugins = Sets.newHashSet();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!plugins.containsKey(mandatoryPlugin) && !sitePlugins.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                throw new ElasticSearchException("Missing mandatory plugins [" + Strings.collectionToDelimitedString(missingPlugins, ", ") + "]");
            }
        }

        logger.info("loaded {}, sites {}", plugins.keySet(), sitePlugins);

        this.plugins = ImmutableMap.copyOf(plugins);

        MapBuilder<Plugin, List<OnModuleReference>> onModuleReferences = MapBuilder.newMapBuilder();
        for (Plugin plugin : plugins.values()) {
            List<OnModuleReference> list = Lists.newArrayList();
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
                list.add(new OnModuleReference(moduleClass, method));
            }
            if (!list.isEmpty()) {
                onModuleReferences.put(plugin, list);
            }
        }
        this.onModuleReferences = onModuleReferences.immutableMap();

        this.refreshInterval = componentSettings.getAsTime("info_refresh_interval", TimeValue.timeValueSeconds(10));

    }

    public ImmutableMap<String, Plugin> plugins() {
        return plugins;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            processModule(module);
        }
    }

    public void processModule(Module module) {
        for (Plugin plugin : plugins().values()) {
            plugin.processModule(module);
            // see if there are onModule references
            List<OnModuleReference> references = onModuleReferences.get(plugin);
            if (references != null) {
                for (OnModuleReference reference : references) {
                    if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                        try {
                            reference.onModuleMethod.invoke(plugin, module);
                        } catch (Exception e) {
                            logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.name());
                        }
                    }
                }
            }
        }
    }

    public Settings updatedSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(this.settings);
        for (Plugin plugin : plugins.values()) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
    }

    public Collection<Class<? extends Module>> modules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.modules());
        }
        return modules;
    }

    public Collection<Module> modules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.modules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> services() {
        List<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            services.addAll(plugin.services());
        }
        return services;
    }

    public Collection<Class<? extends Module>> indexModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.indexModules());
        }
        return modules;
    }

    public Collection<Module> indexModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.indexModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> indexServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            services.addAll(plugin.indexServices());
        }
        return services;
    }

    public Collection<Class<? extends Module>> shardModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.shardModules());
        }
        return modules;
    }

    public Collection<Module> shardModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.shardModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            services.addAll(plugin.shardServices());
        }
        return services;
    }

    /**
     * Get information about plugins (jvm and site plugins).
     * Information are cached for 10 seconds by default. Modify `plugins.info_refresh_interval` property if needed.
     * Setting `plugins.info_refresh_interval` to `-1` will cause infinite caching.
     * Setting `plugins.info_refresh_interval` to `0` will disable caching.
     * @return List of plugins information
     */
    synchronized public PluginsInfo info() {
        if (refreshInterval.millis() != 0) {
            if (cachedPluginsInfo != null &&
                    (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve plugins info");
                return cachedPluginsInfo;
            }
            lastRefresh = System.currentTimeMillis();
        }

        if (logger.isTraceEnabled()) logger.trace("starting to fetch info on plugins");
        cachedPluginsInfo = new PluginsInfo();

        // We create a map to have only unique values
        Set<String> plugins = new HashSet<String>();

        for (Plugin plugin : plugins().values()) {
            // We should detect if the plugin has also an embedded _site structure
            File siteFile = new File(new File(environment.pluginsFile(), plugin.name()), "_site");
            boolean isSite = siteFile.exists() && siteFile.isDirectory();
            if (logger.isTraceEnabled()) logger.trace("found a jvm plugin [{}], [{}]{}",
                    plugin.name(), plugin.description(), isSite ? ": with _site structure" : "");
            cachedPluginsInfo.add(new PluginInfo(plugin.name(), plugin.description(), isSite, true));
            plugins.add(plugin.name());
        }

        File pluginsFile = environment.pluginsFile();
        if (!pluginsFile.exists()) {
            return cachedPluginsInfo;
        }
        if (!pluginsFile.isDirectory()) {
            return cachedPluginsInfo;
        }

        File[] pluginsFiles = pluginsFile.listFiles();
        if (pluginsFiles != null) {
            for (File plugin : pluginsFiles) {
                // We skip already known jvm plugins
                if (!plugins.contains(plugin.getName())) {
                    File sitePluginDir = new File(plugin, "_site");
                    if (sitePluginDir.exists()) {
                        String name = plugin.getName();
                        String description = "No description found for " + name + ".";

                        // We check if es-plugin.properties exists in plugin/_site dir
                        File pluginPropFile = new File(sitePluginDir, ES_PLUGIN_PROPERTIES);
                        if (pluginPropFile.exists()) {

                            Properties pluginProps = new Properties();
                            InputStream is = null;
                            try {
                                is = new FileInputStream(pluginPropFile.getAbsolutePath());
                                pluginProps.load(is);
                                description = pluginProps.getProperty("description");
                            } catch (Exception e) {
                                logger.warn("failed to load plugin description from [" +
                                        pluginPropFile.getAbsolutePath() + "]", e);
                            } finally {
                                if (is != null) {
                                    try {
                                        is.close();
                                    } catch (IOException e) {
                                        // ignore
                                    }
                                }
                            }
                        }

                        if (logger.isTraceEnabled()) logger.trace("found a site plugin [{}], [{}]",
                                name, description);
                        cachedPluginsInfo.add(new PluginInfo(name, description, true, false));
                    }
                }
            }
        }


        return cachedPluginsInfo;
    }

    private void loadPluginsIntoClassLoader() {
        File pluginsFile = environment.pluginsFile();
        if (!pluginsFile.exists()) {
            return;
        }
        if (!pluginsFile.isDirectory()) {
            return;
        }

        ClassLoader classLoader = settings.getClassLoader();
        Class classLoaderClass = classLoader.getClass();
        Method addURL = null;
        while (!classLoaderClass.equals(Object.class)) {
            try {
                addURL = classLoaderClass.getDeclaredMethod("addURL", URL.class);
                addURL.setAccessible(true);
                break;
            } catch (NoSuchMethodException e) {
                // no method, try the parent
                classLoaderClass = classLoaderClass.getSuperclass();
            }
        }
        if (addURL == null) {
            logger.debug("failed to find addURL method on classLoader [" + classLoader + "] to add methods");
            return;
        }

        File[] pluginsFiles = pluginsFile.listFiles();
        if (pluginsFile != null) {
            for (File pluginFile : pluginsFiles) {
                if (pluginFile.isDirectory()) {
                    logger.trace("--- adding plugin [" + pluginFile.getAbsolutePath() + "]");
                    try {
                        // add the root
                        addURL.invoke(classLoader, pluginFile.toURI().toURL());
                        // gather files to add
                        List<File> libFiles = Lists.newArrayList();
                        if (pluginFile.listFiles() != null) {
                            libFiles.addAll(Arrays.asList(pluginFile.listFiles()));
                        }
                        File libLocation = new File(pluginFile, "lib");
                        if (libLocation.exists() && libLocation.isDirectory() && libLocation.listFiles() != null) {
                            libFiles.addAll(Arrays.asList(libLocation.listFiles()));
                        }

                        // if there are jars in it, add it as well
                        for (File libFile : libFiles) {
                            if (!(libFile.getName().endsWith(".jar") || libFile.getName().endsWith(".zip"))) {
                                continue;
                            }
                            addURL.invoke(classLoader, libFile.toURI().toURL());
                        }
                    } catch (Exception e) {
                        logger.warn("failed to add plugin [" + pluginFile + "]", e);
                    }
                }
            }
        } else {
            logger.debug("failed to list plugins from {}. Check your right access.", pluginsFile.getAbsolutePath());
        }
    }

    private Map<String, Plugin> loadPluginsFromClasspath(Settings settings) {
        Map<String, Plugin> plugins = newHashMap();
        Enumeration<URL> pluginUrls = null;
        try {
            pluginUrls = settings.getClassLoader().getResources(ES_PLUGIN_PROPERTIES);
        } catch (IOException e) {
            logger.warn("failed to find plugins from classpath", e);
            return ImmutableMap.of();
        }
        while (pluginUrls.hasMoreElements()) {
            URL pluginUrl = pluginUrls.nextElement();
            Properties pluginProps = new Properties();
            InputStream is = null;
            try {
                is = pluginUrl.openStream();
                pluginProps.load(is);
                String pluginClassName = pluginProps.getProperty("plugin");
                Plugin plugin = loadPlugin(pluginClassName, settings);
                plugins.put(plugin.name(), plugin);
            } catch (Exception e) {
                logger.warn("failed to load plugin from [" + pluginUrl + "]", e);
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
        return plugins;
    }

    private Plugin loadPlugin(String className, Settings settings) {
        try {
            Class<? extends Plugin> pluginClass = (Class<? extends Plugin>) settings.getClassLoader().loadClass(className);
            try {
                return pluginClass.getConstructor(Settings.class).newInstance(settings);
            } catch (NoSuchMethodException e) {
                try {
                    return pluginClass.getConstructor().newInstance();
                } catch (NoSuchMethodException e1) {
                    throw new ElasticSearchException("No constructor for [" + pluginClass + "]. A plugin class must " +
                            "have either an empty default constructor or a single argument constructor accepting a " +
                            "Settings instance");
                }
            }

        } catch (Exception e) {
            throw new ElasticSearchException("Failed to load plugin class [" + className + "]", e);
        }

    }
}
