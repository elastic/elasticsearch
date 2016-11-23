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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.threadpool.ExecutorBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginsService extends AbstractComponent {

    /**
     * We keep around a list of plugins and modules
     */
    private final List<Tuple<PluginInfo, Plugin>> plugins;
    private final PluginsAndModules info;
    public static final Setting<List<String>> MANDATORY_SETTING =
        Setting.listSetting("plugin.mandatory", Collections.emptyList(), Function.identity(), Property.NodeScope);

    public List<Setting<?>> getPluginSettings() {
        return plugins.stream().flatMap(p -> p.v2().getSettings().stream()).collect(Collectors.toList());
    }

    public List<String> getPluginSettingsFilter() {
        return plugins.stream().flatMap(p -> p.v2().getSettingsFilter().stream()).collect(Collectors.toList());
    }

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public PluginsService(Settings settings, Path modulesDirectory, Path pluginsDirectory, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(settings);

        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = new ArrayList<>();
        List<PluginInfo> pluginsList = new ArrayList<>();
        // first we load plugins that are on the classpath. this is for tests and transport clients
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings);
            PluginInfo pluginInfo = new PluginInfo(pluginClass.getName(), "classpath plugin", "NA", pluginClass.getName());
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new Tuple<>(pluginInfo, plugin));
            pluginsList.add(pluginInfo);
        }

        List<PluginInfo> modulesList = new ArrayList<>();
        // load modules
        if (modulesDirectory != null) {
            try {
                List<Bundle> bundles = getModuleBundles(modulesDirectory);
                List<Tuple<PluginInfo, Plugin>> loaded = loadBundles(bundles);
                pluginsLoaded.addAll(loaded);
                for (Tuple<PluginInfo, Plugin> module : loaded) {
                    modulesList.add(module.v1());
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // now, find all the ones that are in plugins/
        if (pluginsDirectory != null) {
            try {
                List<Bundle> bundles = getPluginBundles(pluginsDirectory);
                List<Tuple<PluginInfo, Plugin>> loaded = loadBundles(bundles);
                pluginsLoaded.addAll(loaded);
                for (Tuple<PluginInfo, Plugin> plugin : loaded) {
                    pluginsList.add(plugin.v1());
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        this.info = new PluginsAndModules(pluginsList, modulesList);
        this.plugins = Collections.unmodifiableList(pluginsLoaded);

        // We need to build a List of plugins for checking mandatory plugins
        Set<String> pluginsNames = new HashSet<>();
        for (Tuple<PluginInfo, Plugin> tuple : this.plugins) {
            pluginsNames.add(tuple.v1().getName());
        }

        // Checking expected plugins
        List<String> mandatoryPlugins = MANDATORY_SETTING.get(settings);
        if (mandatoryPlugins.isEmpty() == false) {
            Set<String> missingPlugins = new HashSet<>();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!pluginsNames.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                throw new ElasticsearchException("Missing mandatory plugins [" + Strings.collectionToDelimitedString(missingPlugins, ", ") + "]");
            }
        }

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        logPluginInfo(info.getModuleInfos(), "module", logger);
        logPluginInfo(info.getPluginInfos(), "plugin", logger);
    }

    private static void logPluginInfo(final List<PluginInfo> pluginInfos, final String type, final Logger logger) {
        assert pluginInfos != null;
        if (pluginInfos.isEmpty()) {
            logger.info("no " + type + "s loaded");
        } else {
            for (final String name : pluginInfos.stream().map(PluginInfo::getName).sorted().collect(Collectors.toList())) {
                logger.info("loaded " + type + " [" + name + "]");
            }
        }
    }

    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            Settings settings = plugin.v2().additionalSettings();
            for (String setting : settings.getAsMap().keySet()) {
                String oldPlugin = foundSettings.put(setting, plugin.v1().getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException("Cannot have additional setting [" + setting + "] " +
                        "in plugin [" + plugin.v1().getName() + "], already added in plugin [" + oldPlugin + "]");
                }
            }
            builder.put(settings);
        }
        return builder.put(this.settings).build();
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().createGuiceModules());
        }
        return modules;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final ArrayList<ExecutorBuilder<?>> builders = new ArrayList<>();
        for (final Tuple<PluginInfo, Plugin> plugin : plugins) {
            builders.addAll(plugin.v2().getExecutorBuilders(settings));
        }
        return builders;
    }

    /** Returns all classes injected into guice by plugins which extend {@link LifecycleComponent}. */
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().getGuiceServiceClasses());
        }
        return services;
    }

    public void onIndexModule(IndexModule indexModule) {
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            plugin.v2().onIndexModule(indexModule);
        }
    }

    /**
     * Get information about plugins and modules
     */
    public PluginsAndModules info() {
        return info;
    }

    // a "bundle" is a group of plugins in a single classloader
    // really should be 1-1, but we are not so fortunate
    static class Bundle {
        List<PluginInfo> plugins = new ArrayList<>();
        List<URL> urls = new ArrayList<>();
    }

    // similar in impl to getPluginBundles, but DO NOT try to make them share code.
    // we don't need to inherit all the leniency, and things are different enough.
    static List<Bundle> getModuleBundles(Path modulesDirectory) throws IOException {
        // damn leniency
        if (Files.notExists(modulesDirectory)) {
            return Collections.emptyList();
        }
        List<Bundle> bundles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(modulesDirectory)) {
            for (Path module : stream) {
                if (FileSystemUtils.isHidden(module)) {
                    continue; // skip over .DS_Store etc
                }
                PluginInfo info = PluginInfo.readFromProperties(module);
                Bundle bundle = new Bundle();
                bundle.plugins.add(info);
                // gather urls for jar files
                try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(module, "*.jar")) {
                    for (Path jar : jarStream) {
                        // normalize with toRealPath to get symlinks out of our hair
                        bundle.urls.add(jar.toRealPath().toUri().toURL());
                    }
                }
                bundles.add(bundle);
            }
        }
        return bundles;
    }

    static List<Bundle> getPluginBundles(Path pluginsDirectory) throws IOException {
        Logger logger = Loggers.getLogger(PluginsService.class);

        // TODO: remove this leniency, but tests bogusly rely on it
        if (!isAccessibleDirectory(pluginsDirectory, logger)) {
            return Collections.emptyList();
        }

        List<Bundle> bundles = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory)) {
            for (Path plugin : stream) {
                if (FileSystemUtils.isHidden(plugin)) {
                    logger.trace("--- skip hidden plugin file[{}]", plugin.toAbsolutePath());
                    continue;
                }
                logger.trace("--- adding plugin [{}]", plugin.toAbsolutePath());
                final PluginInfo info;
                try {
                    info = PluginInfo.readFromProperties(plugin);
                } catch (IOException e) {
                    throw new IllegalStateException("Could not load plugin descriptor for existing plugin ["
                        + plugin.getFileName() + "]. Was the plugin built before 2.0?", e);
                }

                List<URL> urls = new ArrayList<>();
                try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(plugin, "*.jar")) {
                    for (Path jar : jarStream) {
                        // normalize with toRealPath to get symlinks out of our hair
                        urls.add(jar.toRealPath().toUri().toURL());
                    }
                }
                final Bundle bundle = new Bundle();
                bundles.add(bundle);
                bundle.plugins.add(info);
                bundle.urls.addAll(urls);
            }
        }

        return bundles;
    }

    private List<Tuple<PluginInfo,Plugin>> loadBundles(List<Bundle> bundles) {
        List<Tuple<PluginInfo, Plugin>> plugins = new ArrayList<>();

        for (Bundle bundle : bundles) {
            // jar-hell check the bundle against the parent classloader
            // pluginmanager does it, but we do it again, in case lusers mess with jar files manually
            try {
                final List<URL> jars = new ArrayList<>();
                jars.addAll(Arrays.asList(JarHell.parseClassPath()));
                jars.addAll(bundle.urls);
                JarHell.checkJarHell(jars.toArray(new URL[0]));
            } catch (Exception e) {
                throw new IllegalStateException("failed to load bundle " + bundle.urls + " due to jar hell", e);
            }

            // create a child to load the plugins in this bundle
            ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), getClass().getClassLoader());
            for (PluginInfo pluginInfo : bundle.plugins) {
                // reload lucene SPI with any new services from the plugin
                reloadLuceneSPI(loader);
                final Class<? extends Plugin> pluginClass = loadPluginClass(pluginInfo.getClassname(), loader);
                final Plugin plugin = loadPlugin(pluginClass, settings);
                plugins.add(new Tuple<>(pluginInfo, plugin));
            }
        }

        return Collections.unmodifiableList(plugins);
    }

    /**
     * Reloads all Lucene SPI implementations using the new classloader.
     * This method must be called after the new classloader has been created to
     * register the services for use.
     */
    static void reloadLuceneSPI(ClassLoader loader) {
        // do NOT change the order of these method calls!

        // Codecs:
        PostingsFormat.reloadPostingsFormats(loader);
        DocValuesFormat.reloadDocValuesFormats(loader);
        Codec.reloadCodecs(loader);
        // Analysis:
        CharFilterFactory.reloadCharFilters(loader);
        TokenFilterFactory.reloadTokenFilters(loader);
        TokenizerFactory.reloadTokenizers(loader);
    }

    private Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new ElasticsearchException("Could not find plugin class [" + className + "]", e);
        }
    }

    private Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings) {
        try {
            try {
                return pluginClass.getConstructor(Settings.class).newInstance(settings);
            } catch (NoSuchMethodException e) {
                try {
                    return pluginClass.getConstructor().newInstance();
                } catch (NoSuchMethodException e1) {
                    throw new ElasticsearchException("No constructor for [" + pluginClass + "]. A plugin class must " +
                        "have either an empty default constructor or a single argument constructor accepting a " +
                        "Settings instance");
                }
            }
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    public <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.v2().getClass()))
            .map(p -> ((T)p.v2())).collect(Collectors.toList());
    }
}
