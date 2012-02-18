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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.CloseableIndexComponent;

import java.io.File;
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

    private final Environment environment;

    private final ImmutableMap<String, Plugin> plugins;

    private final ImmutableMap<Plugin, List<OnModuleReference>> onModuleReferences;

    static class OnModuleReference {
        public final Class<? extends Module> moduleClass;
        public final Method onModuleMethod;

        OnModuleReference(Class<? extends Module> moduleClass, Method onModuleMethod) {
            this.moduleClass = moduleClass;
            this.onModuleMethod = onModuleMethod;
        }
    }

    @Inject
    public PluginsService(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;

        loadPluginsIntoClassLoader();

        // first, find all the ones that are in the classpath
        Map<String, Plugin> plugins = Maps.newHashMap();
        plugins.putAll(loadPluginsFromClasspath(settings));
        Set<String> sitePlugins = sitePlugins();

        String[] mandatoryPlugins = settings.getAsArray("plugin.mandatory", null);
        if (mandatoryPlugins != null) {
            Set<String> missingPlugins = Sets.newHashSet();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!plugins.containsKey(mandatoryPlugin) && !sitePlugins.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                throw new ElasticSearchException("Missing mandatory plugins " + missingPlugins);
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

    private Set<String> sitePlugins() {
        File pluginsFile = environment.pluginsFile();
        Set<String> sitePlugins = Sets.newHashSet();
        if (!pluginsFile.exists()) {
            return sitePlugins;
        }
        if (!pluginsFile.isDirectory()) {
            return sitePlugins;
        }
        File[] pluginsFiles = pluginsFile.listFiles();
        for (File pluginFile : pluginsFiles) {
            if (new File(pluginFile, "_site").exists()) {
                sitePlugins.add(pluginFile.getName());
            }
        }
        return sitePlugins;
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
    }

    private Map<String, Plugin> loadPluginsFromClasspath(Settings settings) {
        Map<String, Plugin> plugins = newHashMap();
        Enumeration<URL> pluginUrls = null;
        try {
            pluginUrls = settings.getClassLoader().getResources("es-plugin.properties");
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
                String sPluginClass = pluginProps.getProperty("plugin");
                Class<? extends Plugin> pluginClass = (Class<? extends Plugin>) settings.getClassLoader().loadClass(sPluginClass);
                Plugin plugin;
                try {
                    plugin = pluginClass.getConstructor(Settings.class).newInstance(settings);
                } catch (NoSuchMethodException e) {
                    try {
                        plugin = pluginClass.getConstructor().newInstance();
                    } catch (NoSuchMethodException e1) {
                        throw new ElasticSearchException("No constructor for [" + pluginClass + "]");
                    }
                }
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
}
