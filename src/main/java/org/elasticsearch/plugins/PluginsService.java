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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CachedReference;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.CloseableIndexComponent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

/**
 *
 */
public class PluginsService extends AbstractComponent {

    private static final String ES_PLUGIN_PROPERTIES = "es-plugin.properties";

    private final Environment environment;

    private final ImmutableList<JvmPlugin> jvmPlugins;

    private CachedReference<PluginsInfo> pluginsInfo;

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system
     * @param environment The environment of the system
     */
    public PluginsService(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;

        ImmutableList.Builder<JvmPlugin> jvmPluginsList = ImmutableList.builder();
        loadJvmPluginsFromSettings(settings, jvmPluginsList);
        loadJvmPluginsFromClasspath(settings, jvmPluginsList);
        jvmPlugins = jvmPluginsList.build();

        Set<PluginInfo> sitesInfos = loadSiteOnlyPluginsInfos();

        verifyMandatoryPlugins(settings, jvmPlugins, sitesInfos);

        TimeValue refreshInterval = componentSettings.getAsTime("info_refresh_interval", TimeValue.timeValueSeconds(10));
        pluginsInfo = new CachedReference<PluginsInfo>(refreshInterval, new CachedReference.Loader<PluginsInfo>() {
            @Override
            public PluginsInfo load() {
                if (logger.isTraceEnabled()) {
                    logger.trace("starting to fetch info on plugins");
                }
                PluginsInfo pluginsInfo = new PluginsInfo();

                for (JvmPlugin plugin : jvmPlugins) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("adding jvm plugin [{}]", plugin.info());
                    }
                    pluginsInfo.add(plugin.info());
                }

                // We reload site plugins (in case of some changes)
                for (PluginInfo info : loadSiteOnlyPluginsInfos()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("adding site plugin [{}]", info);
                    }
                    pluginsInfo.add(info);
                }

                return pluginsInfo;
            }
        });

        // Collecting the names of the jvm plugins and the site plugins for logging
        Set<String> sitePluginsNames = Sets.newHashSet();
        Set<String> jvmPluginsNames = Sets.newHashSet();
        for (JvmPlugin plugin : this.jvmPlugins) {
            jvmPluginsNames.add(plugin.name());
            if (plugin.info().isSite()) {
                sitePluginsNames.add(plugin.name());
            }
        }
        for (PluginInfo siteInfo : sitesInfos) {
            sitePluginsNames.add(siteInfo.getName());
        }
        logger.info("loaded {}, sites {}", jvmPluginsNames, sitePluginsNames);

    }

    public ImmutableList<JvmPlugin> plugins() {
        return jvmPlugins;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            processModule(module);
        }
    }

    public void processModule(Module module) {
        for (JvmPlugin plugin : jvmPlugins) {
            plugin.processModule(module);
        }
    }

    public Settings updatedSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(this.settings);
        for (JvmPlugin plugin : jvmPlugins) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
    }

    public Collection<Class<? extends Module>> modules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.modules());
        }
        return modules;
    }

    public Collection<Module> modules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.modules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> services() {
        List<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            services.addAll(plugin.services());
        }
        return services;
    }

    public Collection<Class<? extends Module>> indexModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.indexModules());
        }
        return modules;
    }

    public Collection<Module> indexModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.indexModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> indexServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            services.addAll(plugin.indexServices());
        }
        return services;
    }

    public Collection<Class<? extends Module>> shardModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.shardModules());
        }
        return modules;
    }

    public Collection<Module> shardModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
            modules.addAll(plugin.shardModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (JvmPlugin plugin : jvmPlugins) {
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
        return pluginsInfo.get();
    }

    private void loadJvmPluginsFromSettings(Settings settings, ImmutableList.Builder<JvmPlugin> plugins) {
        String[] defaultPluginsClasses = settings.getAsArray("plugin.types");
        for (String pluginClass : defaultPluginsClasses) {
            Plugin plugin = loadPlugin(pluginClass, settings);
            PluginInfo pluginInfo = new PluginInfo(plugin.name(), plugin.description(), hasSite(plugin.name()), true, PluginInfo.VERSION_NOT_AVAILABLE);
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from settings [{}]", pluginInfo);
            }
            plugins.add(new JvmPlugin(plugin, pluginInfo, logger));
        }
    }

    private void loadJvmPluginsFromClasspath(Settings settings, ImmutableList.Builder<JvmPlugin> plugins) {
        loadJvmPluginsIntoClassLoader();
        // Trying JVM plugins: looking for es-plugin.properties files
        try {
            Enumeration<URL> pluginUrls = settings.getClassLoader().getResources(ES_PLUGIN_PROPERTIES);
            while (pluginUrls.hasMoreElements()) {
                URL pluginUrl = pluginUrls.nextElement();
                Properties pluginProps = new Properties();
                InputStream is = null;
                try {
                    is = pluginUrl.openStream();
                    pluginProps.load(is);
                    String pluginClassName = pluginProps.getProperty("plugin");
                    String pluginVersion = pluginProps.getProperty("version", PluginInfo.VERSION_NOT_AVAILABLE);
                    Plugin plugin = loadPlugin(pluginClassName, settings);

                    // Is it a site plugin as well? Does it have also an embedded _site structure
                    File siteFile = new File(new File(environment.pluginsFile(), plugin.name()), "_site");
                    boolean isSite = siteFile.exists() && siteFile.isDirectory();
                    if (logger.isTraceEnabled()) {
                        logger.trace("found a jvm plugin [{}], [{}]{}",
                                plugin.name(), plugin.description(), isSite ? ": with _site structure" : "");
                    }

                    PluginInfo pluginInfo = new PluginInfo(plugin.name(), plugin.description(), isSite, true, pluginVersion);

                    plugins.add(new JvmPlugin(plugin, pluginInfo, logger));

                } catch (Throwable e) {
                    logger.warn("failed to load plugin from [" + pluginUrl + "]", e);
                } finally {
                    IOUtils.closeWhileHandlingException(is);
                }
            }
        } catch (IOException e) {
            logger.warn("failed to find jvm plugins from classpath", e);
        }
    }

    private void loadJvmPluginsIntoClassLoader() {
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
                    if (logger.isTraceEnabled()) {
                        logger.trace("--- adding plugin [" + pluginFile.getAbsolutePath() + "]");
                    }
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
                    } catch (Throwable e) {
                        logger.warn("failed to add plugin [" + pluginFile + "]", e);
                    }
                }
            }
        } else {
            logger.debug("failed to list plugins from {}. Check your right access.", pluginsFile.getAbsolutePath());
        }
    }

    private Set<PluginInfo> loadSiteOnlyPluginsInfos() {
        Set<String> loadedJvmPlugins = Sets.newHashSet();

        // Already known jvm plugins are ignored
        for(JvmPlugin plugin : jvmPlugins) {
            if (plugin.info().isSite()) {
                loadedJvmPlugins.add(plugin.name());
            }
        }

        // Let's try to find all _site plugins we did not already found
        File pluginsFile = environment.pluginsFile();

        if (!pluginsFile.exists() || !pluginsFile.isDirectory()) {
            return Collections.emptySet();
        }

        Set<PluginInfo> siteInfos = Sets.newHashSet();
        for (File pluginFile : pluginsFile.listFiles()) {
            if (!loadedJvmPlugins.contains(pluginFile.getName())) {
                File sitePluginDir = new File(pluginFile, "_site");
                if (sitePluginDir.exists()) {
                    // We have a _site plugin. Let's try to get more information on it
                    String name = pluginFile.getName();
                    String version = PluginInfo.VERSION_NOT_AVAILABLE;
                    String description = PluginInfo.DESCRIPTION_NOT_AVAILABLE;

                    // We check if es-plugin.properties exists in plugin/_site dir
                    File pluginPropFile = new File(sitePluginDir, ES_PLUGIN_PROPERTIES);
                    if (pluginPropFile.exists()) {

                        Properties pluginProps = new Properties();
                        InputStream is = null;
                        try {
                            is = new FileInputStream(pluginPropFile.getAbsolutePath());
                            pluginProps.load(is);
                            description = pluginProps.getProperty("description", PluginInfo.DESCRIPTION_NOT_AVAILABLE);
                            version = pluginProps.getProperty("version", PluginInfo.VERSION_NOT_AVAILABLE);
                        } catch (Exception e) {
                            // Can not load properties for this site plugin. Ignoring.
                            logger.debug("can not load {} file.", e, ES_PLUGIN_PROPERTIES);
                        } finally {
                            IOUtils.closeWhileHandlingException(is);
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("found a site plugin name [{}], version [{}], description [{}]",
                                name, version, description);
                    }
                    siteInfos.add(new PluginInfo(name, description, true, false, version));
                }
            }
        }
        return siteInfos;
    }

    private static void verifyMandatoryPlugins(Settings settings, List<JvmPlugin> jvmPlugins, Set<PluginInfo> siteInfos) {
        // Checking expected plugins
        String[] mandatoryPluginsNames = settings.getAsArray("plugin.mandatory", null);
        if (mandatoryPluginsNames == null) {
            return;
        }
        Set<String> mandatoryPlugins = Sets.newHashSet(mandatoryPluginsNames);
        for (JvmPlugin plugin : jvmPlugins) {
            mandatoryPlugins.remove(plugin.name());
        }
        for (PluginInfo siteInfo : siteInfos) {
            mandatoryPlugins.remove(siteInfo.getName());
        }
        if (!mandatoryPlugins.isEmpty()) {
            throw new ElasticsearchException("Missing mandatory plugins [" + Strings.collectionToDelimitedString(mandatoryPlugins, ", ") + "]");
        }
    }

    /**
     * @param name plugin name
     * @return if this jvm plugin has also a _site structure
     */
    private boolean hasSite(String name) {
        // Let's try to find all _site plugins we did not already found
        File pluginsFile = environment.pluginsFile();

        if (!pluginsFile.exists() || !pluginsFile.isDirectory()) {
            return false;
        }

        File sitePluginDir = new File(pluginsFile, name + "/_site");
        return sitePluginDir.exists();
    }

    private Plugin loadPlugin(String className, Settings settings) {
        try {
            Class<? extends Plugin> pluginClass = (Class<? extends Plugin>) settings.getClassLoader().loadClass(className);
            Plugin plugin;
            try {
                plugin = pluginClass.getConstructor(Settings.class).newInstance(settings);
            } catch (NoSuchMethodException e) {
                try {
                    plugin = pluginClass.getConstructor().newInstance();
                } catch (NoSuchMethodException e1) {
                    throw new ElasticsearchException("No constructor for [" + pluginClass + "]. A plugin class must " +
                            "have either an empty default constructor or a single argument constructor accepting a " +
                            "Settings instance");
                }
            }

            return plugin;

        } catch (Throwable e) {
            throw new ElasticsearchException("Failed to load plugin class [" + className + "]", e);
        }
    }
}
