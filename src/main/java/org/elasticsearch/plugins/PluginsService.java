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

import com.google.common.collect.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
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
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

/**
 *
 */
public class PluginsService extends AbstractComponent {
    private static final String ES_PLUGIN_PROPERTIES = "es-plugin.properties";

    private final Environment environment;

    /**
     * We keep around a list of jvm plugins
     */
    private final ImmutableList<Tuple<PluginInfo, Plugin>> plugins;

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

        ImmutableList.Builder<Tuple<PluginInfo, Plugin>> tupleBuilder = ImmutableList.builder();

        // first we load all the default plugins from the settings
        String[] defaultPluginsClasses = settings.getAsArray("plugin.types");
        for (String pluginClass : defaultPluginsClasses) {
            Plugin plugin = PluginUtils.loadPlugin(pluginClass, settings, settings.getClassLoader());
            PluginInfo pluginInfo = new PluginInfo(plugin.name(), plugin.description(), hasSite(plugin.name()), true, PluginInfo.VERSION_NOT_AVAILABLE, false);
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from settings [{}]", pluginInfo);
            }
            tupleBuilder.add(new Tuple<>(pluginInfo, plugin));
        }

        // now, find all the ones that are in the classpath
        tupleBuilder.addAll(loadPlugins());
        this.plugins = tupleBuilder.build();

        // We need to build a List of jvm and site plugins for checking mandatory plugins
        Map<String, Plugin> jvmPlugins = Maps.newHashMap();
        List<String> sitePlugins = Lists.newArrayList();

        for (Tuple<PluginInfo, Plugin> tuple : this.plugins) {
            jvmPlugins.put(tuple.v2().name(), tuple.v2());
            if (tuple.v1().isSite()) {
                sitePlugins.add(tuple.v1().getName());
            }
        }

        // we load site plugins
        ImmutableList<Tuple<PluginInfo, Plugin>> tuples = loadSitePlugins();
        for (Tuple<PluginInfo, Plugin> tuple : tuples) {
            sitePlugins.add(tuple.v1().getName());
        }

        // Checking expected plugins
        String[] mandatoryPlugins = settings.getAsArray("plugin.mandatory", null);
        if (mandatoryPlugins != null) {
            Set<String> missingPlugins = Sets.newHashSet();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!jvmPlugins.containsKey(mandatoryPlugin) && !sitePlugins.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                throw new ElasticsearchException("Missing mandatory plugins [" + Strings.collectionToDelimitedString(missingPlugins, ", ") + "]");
            }
        }

        logger.info("loaded {}, sites {}", jvmPlugins.keySet(), sitePlugins);

        MapBuilder<Plugin, List<OnModuleReference>> onModuleReferences = MapBuilder.newMapBuilder();
        for (Plugin plugin : jvmPlugins.values()) {
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

    public ImmutableList<Tuple<PluginInfo, Plugin>> plugins() {
        return plugins;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            processModule(module);
        }
    }

    public void processModule(Module module) {
        for (Tuple<PluginInfo, Plugin> plugin : plugins()) {
            plugin.v2().processModule(module);
            // see if there are onModule references
            List<OnModuleReference> references = onModuleReferences.get(plugin.v2());
            if (references != null) {
                for (OnModuleReference reference : references) {
                    if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                        try {
                            reference.onModuleMethod.invoke(plugin.v2(), module);
                        } catch (Exception e) {
                            logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.v2().name());
                        }
                    }
                }
            }
        }
    }

    public Settings updatedSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(this.settings);
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            builder.put(plugin.v2().additionalSettings());
        }
        return builder.build();
    }

    public Collection<Class<? extends Module>> modules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().modules());
        }
        return modules;
    }

    public Collection<Module> modules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().modules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> services() {
        List<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().services());
        }
        return services;
    }

    public Collection<Class<? extends Module>> indexModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().indexModules());
        }
        return modules;
    }

    public Collection<Module> indexModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().indexModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> indexServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().indexServices());
        }
        return services;
    }

    public Collection<Class<? extends Module>> shardModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().shardModules());
        }
        return modules;
    }

    public Collection<Module> shardModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().shardModules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        List<Class<? extends CloseableIndexComponent>> services = Lists.newArrayList();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().shardServices());
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
                if (logger.isTraceEnabled()) {
                    logger.trace("using cache to retrieve plugins info");
                }
                return cachedPluginsInfo;
            }
            lastRefresh = System.currentTimeMillis();
        }

        if (logger.isTraceEnabled()) {
            logger.trace("starting to fetch info on plugins");
        }
        cachedPluginsInfo = new PluginsInfo();

        // We first add all JvmPlugins
        for (Tuple<PluginInfo, Plugin> plugin : this.plugins) {
            if (logger.isTraceEnabled()) {
                logger.trace("adding jvm plugin [{}]", plugin.v1());
            }
            cachedPluginsInfo.add(plugin.v1());
        }

        // We reload site plugins (in case of some changes)
        for (Tuple<PluginInfo, Plugin> plugin : loadSitePlugins()) {
            if (logger.isTraceEnabled()) {
                logger.trace("adding site plugin [{}]", plugin.v1());
            }
            cachedPluginsInfo.add(plugin.v1());
        }

        return cachedPluginsInfo;
    }

    private List<Tuple<PluginInfo,Plugin>> loadPlugins() {
        File pluginsFile = environment.pluginsFile();
        if (!isAccessibleDirectory(pluginsFile, logger)) {
            return Collections.emptyList();
        }

        List<Tuple<PluginInfo, Plugin>> pluginData = Lists.newArrayList();

        boolean defaultIsolation = settings.getAsBoolean("plugins.isolation", Boolean.FALSE);
        ClassLoader esClassLoader = settings.getClassLoader();
        Method addURL = null;
        boolean discoveredAddUrl = false;

        File[] pluginsFiles = pluginsFile.listFiles();

        if (pluginsFiles != null) {
            for (File pluginRoot : pluginsFiles) {
                if (isAccessibleDirectory(pluginRoot, logger)) {
                    try {
                        logger.trace("--- adding plugin [" + pluginRoot.getAbsolutePath() + "]");
                        // check isolation
                        List<File> pluginClassPath = PluginUtils.pluginClassPathAsFiles(pluginRoot);
                        List<URL> pluginProperties = PluginUtils.lookupPluginProperties(pluginClassPath);
                        boolean isolated = PluginUtils.lookupIsolation(pluginProperties, defaultIsolation);

                        if (isolated) {
                            logger.trace("--- creating isolated space for plugin [" + pluginRoot.getAbsolutePath() + "]");
                            PluginClassLoader pcl = new PluginClassLoader(PluginUtils.convertFileToUrl(pluginClassPath), esClassLoader);
                            pluginData.addAll(loadPlugin(pluginClassPath, pluginProperties, pcl, true));
                        } else {
                            if (!discoveredAddUrl) {
                                discoveredAddUrl = true;
                                Class<?> esClassLoaderClass = esClassLoader.getClass();

                                while (!esClassLoaderClass.equals(Object.class)) {
                                    try {
                                        addURL = esClassLoaderClass.getDeclaredMethod("addURL", URL.class);
                                        addURL.setAccessible(true);
                                        break;
                                    } catch (NoSuchMethodException e) {
                                        // no method, try the parent
                                        esClassLoaderClass = esClassLoaderClass.getSuperclass();
                                    }
                                }
                            }

                            if (addURL == null) {
                                logger.debug("failed to find addURL method on classLoader [" + esClassLoader + "] to add methods");
                            }
                            else {
                                for (File file : pluginClassPath) {
                                    addURL.invoke(esClassLoader, file.toURI().toURL());
                                }
                                pluginData.addAll(loadPlugin(pluginClassPath, pluginProperties, esClassLoader, false));
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("failed to add plugin [" + pluginRoot.getAbsolutePath() + "]", e);
                    }
                }
            }
        } else {
            logger.debug("failed to list plugins from {}. Check your right access.", pluginsFile.getAbsolutePath());
        }

        return pluginData;
    }

    private Collection<? extends Tuple<PluginInfo, Plugin>> loadPlugin(List<File> pluginClassPath, List<URL> properties, ClassLoader classLoader, boolean isolation) throws Exception {
        List<Tuple<PluginInfo, Plugin>> plugins = Lists.newArrayList();

        Enumeration<URL> entries = Collections.enumeration(properties);
        while (entries.hasMoreElements()) {
            URL pluginUrl = entries.nextElement();
            Properties pluginProps = new Properties();
            InputStream is = null;
            try {
                is = pluginUrl.openStream();
                pluginProps.load(is);
                String pluginClassName = pluginProps.getProperty("plugin");
                if (pluginClassName == null) {
                    throw new IllegalArgumentException("No plugin class specified");
                }
                String pluginVersion = pluginProps.getProperty("version", PluginInfo.VERSION_NOT_AVAILABLE);
                Plugin plugin = PluginUtils.loadPlugin(pluginClassName, settings, classLoader);

                // Is it a site plugin as well? Does it have also an embedded _site structure
                File siteFile = new File(new File(environment.pluginsFile(), plugin.name()), "_site");
                boolean isSite = isAccessibleDirectory(siteFile, logger);
                if (logger.isTraceEnabled()) {
                    logger.trace("found a jvm plugin [{}], [{}]{}",
                            plugin.name(), plugin.description(), isSite ? ": with _site structure" : "");
                }

                PluginInfo pluginInfo = new PluginInfo(plugin.name(), plugin.description(), isSite, true, pluginVersion, isolation);
                plugins.add(new Tuple<>(pluginInfo, plugin));
            } catch (Throwable e) {
                logger.warn("failed to load plugin from [" + pluginUrl + "]", e);
            } finally {
                IOUtils.closeWhileHandlingException(is);
            }
        }

        return plugins;
    }

    private ImmutableList<Tuple<PluginInfo,Plugin>> loadSitePlugins() {
        ImmutableList.Builder<Tuple<PluginInfo, Plugin>> sitePlugins = ImmutableList.builder();
        List<String> loadedJvmPlugins = new ArrayList<>();

        // Already known jvm plugins are ignored
        for(Tuple<PluginInfo, Plugin> tuple : plugins) {
            if (tuple.v1().isSite()) {
                loadedJvmPlugins.add(tuple.v1().getName());
            }
        }

        // Let's try to find all _site plugins we did not already found
        File pluginsFile = environment.pluginsFile();

        if (!pluginsFile.exists() || !pluginsFile.isDirectory()) {
            return sitePlugins.build();
        }

        for (File pluginFile : pluginsFile.listFiles()) {
            if (!loadedJvmPlugins.contains(pluginFile.getName())) {
                File sitePluginDir = new File(pluginFile, "_site");
                if (isAccessibleDirectory(sitePluginDir, logger)) {
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
                    sitePlugins.add(new Tuple<PluginInfo, Plugin>(new PluginInfo(name, description, true, false, version, false), null));
                }
            }
        }

        return sitePlugins.build();
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
        return isAccessibleDirectory(sitePluginDir, logger);
    }
}