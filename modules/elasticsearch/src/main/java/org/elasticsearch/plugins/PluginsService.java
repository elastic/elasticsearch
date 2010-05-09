/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.collect.Maps;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.CloseableIndexComponent;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.inject.Module;
import org.elasticsearch.util.io.Streams;
import org.elasticsearch.util.settings.Settings;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.elasticsearch.util.collect.Maps.*;
import static org.elasticsearch.util.io.FileSystemUtils.*;

/**
 * @author kimchy (shay.banon)
 */
public class PluginsService extends AbstractComponent {

    private final Environment environment;

    private final ImmutableMap<String, Plugin> plugins;

    @Inject public PluginsService(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;

        loadPluginsIntoClassLoader();

        // first, find all the ones that are in the classpath
        Map<String, Plugin> plugins = Maps.newHashMap();
        plugins.putAll(loadPluginsFromClasspath(settings));

        logger.info("Loaded {}", plugins.keySet());

        this.plugins = ImmutableMap.copyOf(plugins);
    }

    public Settings updatedSettings() {
        return this.settings;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            for (Plugin plugin : plugins.values()) {
                plugin.processModule(module);
            }
        }
    }

    public Collection<Class<? extends Module>> modules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            modules.addAll(plugin.modules());
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

    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
            services.addAll(plugin.shardServices());
        }
        return services;
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
            logger.debug("Failed to find addURL method on classLoader [" + classLoader + "] to add methods");
            return;
        }

        File[] pluginsFiles = pluginsFile.listFiles();
        for (File pluginFile : pluginsFiles) {
            if (!pluginFile.getName().endsWith(".zip")) {
                continue;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Processing [{}]", pluginFile);
            }

            String pluginNameNoExtension = pluginFile.getName().substring(0, pluginFile.getName().lastIndexOf('.'));
            File extractedPluginDir = new File(new File(environment.workFile(), "plugins"), pluginNameNoExtension);
            extractedPluginDir.mkdirs();

            File stampsDir = new File(new File(environment.workFile(), "plugins"), "_stamps");
            stampsDir.mkdirs();

            boolean extractPlugin = true;
            File stampFile = new File(stampsDir, pluginNameNoExtension + ".stamp");
            if (stampFile.exists()) {
                // read it, and check if its the same size as the pluginFile
                RandomAccessFile raf = null;
                try {
                    raf = new RandomAccessFile(stampFile, "r");
                    long size = raf.readLong();
                    if (size == pluginFile.length()) {
                        extractPlugin = false;
                        if (logger.isTraceEnabled()) {
                            logger.trace("--- No need to extract plugin, same size [" + size + "]");
                        }
                    }
                } catch (Exception e) {
                    // ignore and extract the plugin
                } finally {
                    if (raf != null) {
                        try {
                            raf.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            }

            if (extractPlugin) {
                if (logger.isTraceEnabled()) {
                    logger.trace("--- Extracting plugin to [" + extractedPluginDir + "]");
                }
                deleteRecursively(extractedPluginDir, false);

                ZipFile zipFile = null;
                try {
                    zipFile = new ZipFile(pluginFile);
                    Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
                    while (zipEntries.hasMoreElements()) {
                        ZipEntry zipEntry = zipEntries.nextElement();
                        if (!(zipEntry.getName().endsWith(".jar") || zipEntry.getName().endsWith(".zip"))) {
                            continue;
                        }
                        String name = zipEntry.getName().replace('\\', '/');
                        File target = new File(extractedPluginDir, name);
                        Streams.copy(zipFile.getInputStream(zipEntry), new FileOutputStream(target));
                    }
                } catch (Exception e) {
                    logger.warn("Failed to extract plugin [" + pluginFile + "], ignoring...", e);
                    continue;
                } finally {
                    if (zipFile != null) {
                        try {
                            zipFile.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }

                try {
                    RandomAccessFile raf = new RandomAccessFile(stampFile, "rw");
                    raf.writeLong(pluginFile.length());
                    raf.close();
                } catch (Exception e) {
                    // ignore 
                }

            }

            try {
                for (File jarToAdd : extractedPluginDir.listFiles()) {
                    if (!(jarToAdd.getName().endsWith(".jar") || jarToAdd.getName().endsWith(".zip"))) {
                        continue;
                    }
                    addURL.invoke(classLoader, jarToAdd.toURI().toURL());
                }
            } catch (Exception e) {
                logger.warn("Failed to add plugin [" + pluginFile + "]", e);
            }
        }
    }

    private Map<String, Plugin> loadPluginsFromClasspath(Settings settings) {
        Map<String, Plugin> plugins = newHashMap();
        Enumeration<URL> pluginUrls = null;
        try {
            pluginUrls = settings.getClassLoader().getResources("es-plugin.properties");
        } catch (IOException e) {
            logger.warn("Failed to find plugins from classpath", e);
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
                logger.warn("Failed to load plugin from [" + pluginUrl + "]", e);
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
