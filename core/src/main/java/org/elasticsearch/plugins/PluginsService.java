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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

/**
 *
 */
public class PluginsService extends AbstractComponent {

    /**
     * We keep around a list of plugins and modules
     */
    private final List<Tuple<PluginInfo, Plugin>> plugins;
    private final PluginsAndModules info;

    private final Map<Plugin, List<OnModuleReference>> onModuleReferences;

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
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public PluginsService(Settings settings, Path modulesDirectory, Path pluginsDirectory, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(settings);
        info = new PluginsAndModules();

        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = new ArrayList<>();

        // first we load plugins that are on the classpath. this is for tests and transport clients
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings);
            PluginInfo pluginInfo = new PluginInfo(plugin.name(), plugin.description(), false, "NA", true, pluginClass.getName(), false);
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new Tuple<>(pluginInfo, plugin));
            info.addPlugin(pluginInfo);
        }

        // load modules
        if (modulesDirectory != null) {
            try {
                List<Bundle> bundles = getModuleBundles(modulesDirectory);
                List<Tuple<PluginInfo, Plugin>> loaded = loadBundles(bundles);
                pluginsLoaded.addAll(loaded);
                for (Tuple<PluginInfo, Plugin> module : loaded) {
                    info.addModule(module.v1());
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
                    info.addPlugin(plugin.v1());
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        plugins = Collections.unmodifiableList(pluginsLoaded);

        // We need to build a List of jvm and site plugins for checking mandatory plugins
        Map<String, Plugin> jvmPlugins = new HashMap<>();
        List<String> sitePlugins = new ArrayList<>();

        for (Tuple<PluginInfo, Plugin> tuple : plugins) {
            PluginInfo info = tuple.v1();
            if (info.isJvm()) {
                jvmPlugins.put(info.getName(), tuple.v2());
            }
            if (info.isSite()) {
                sitePlugins.add(info.getName());
            }
        }

        // Checking expected plugins
        String[] mandatoryPlugins = settings.getAsArray("plugin.mandatory", null);
        if (mandatoryPlugins != null) {
            Set<String> missingPlugins = new HashSet<>();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!jvmPlugins.containsKey(mandatoryPlugin) && !sitePlugins.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                throw new ElasticsearchException("Missing mandatory plugins [" + Strings.collectionToDelimitedString(missingPlugins, ", ") + "]");
            }
        }

        // we don't log jars in lib/ we really shouldnt log modules,
        // but for now: just be transparent so we can debug any potential issues
        Set<String> moduleNames = new HashSet<>();
        Set<String> jvmPluginNames = new HashSet<>();
        for (PluginInfo moduleInfo : info.getModuleInfos()) {
            moduleNames.add(moduleInfo.getName());
        }
        for (PluginInfo pluginInfo : info.getPluginInfos()) {
            jvmPluginNames.add(pluginInfo.getName());
        }

        logger.info("modules {}, plugins {}, sites {}", moduleNames, jvmPluginNames, sitePlugins);

        Map<Plugin, List<OnModuleReference>> onModuleReferences = new HashMap<>();
        for (Plugin plugin : jvmPlugins.values()) {
            List<OnModuleReference> list = new ArrayList<>();
            for (Method method : plugin.getClass().getMethods()) {
                if (!method.getName().equals("onModule")) {
                    continue;
                }
                // this is a deprecated final method, so all Plugin subclasses have it
                if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(IndexModule.class)) {
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
                list.add(new OnModuleReference(moduleClass, method));
            }
            if (!list.isEmpty()) {
                onModuleReferences.put(plugin, list);
            }
        }
        this.onModuleReferences = Collections.unmodifiableMap(onModuleReferences);
    }

    private List<Tuple<PluginInfo, Plugin>> plugins() {
        return plugins;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            processModule(module);
        }
    }

    public void processModule(Module module) {
        for (Tuple<PluginInfo, Plugin> plugin : plugins()) {
            // see if there are onModule references
            List<OnModuleReference> references = onModuleReferences.get(plugin.v2());
            if (references != null) {
                for (OnModuleReference reference : references) {
                    if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                        try {
                            reference.onModuleMethod.invoke(plugin.v2(), module);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.v2().name());
                            throw new ElasticsearchException("failed to invoke onModule", e);
                        } catch (Exception e) {
                            logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.v2().name());
                            throw e;
                        }
                    }
                }
            }
        }
    }

    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.settingsBuilder();
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

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            modules.addAll(plugin.v2().nodeModules());
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            services.addAll(plugin.v2().nodeServices());
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
                if (!info.isJvm()) {
                    throw new IllegalStateException("modules must be jvm plugins: " + info);
                }
                if (!info.isIsolated()) {
                    throw new IllegalStateException("modules must be isolated: " + info);
                }
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
        ESLogger logger = Loggers.getLogger(PluginsService.class);

        // TODO: remove this leniency, but tests bogusly rely on it
        if (!isAccessibleDirectory(pluginsDirectory, logger)) {
            return Collections.emptyList();
        }

        List<Bundle> bundles = new ArrayList<>();
        // a special purgatory for plugins that directly depend on each other
        bundles.add(new Bundle());

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
                if (info.isJvm()) {
                    // a jvm plugin: gather urls for jar files
                    try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(plugin, "*.jar")) {
                        for (Path jar : jarStream) {
                            // normalize with toRealPath to get symlinks out of our hair
                            urls.add(jar.toRealPath().toUri().toURL());
                        }
                    }
                }
                final Bundle bundle;
                if (info.isJvm() && info.isIsolated() == false) {
                    bundle = bundles.get(0); // purgatory
                } else {
                    bundle = new Bundle();
                    bundles.add(bundle);
                }
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
                final Plugin plugin;
                if (pluginInfo.isJvm()) {
                    // reload lucene SPI with any new services from the plugin
                    reloadLuceneSPI(loader);
                    Class<? extends Plugin> pluginClass = loadPluginClass(pluginInfo.getClassname(), loader);
                    plugin = loadPlugin(pluginClass, settings);
                } else {
                    plugin = new SitePlugin(pluginInfo.getName(), pluginInfo.getDescription());
                }
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
        } catch (Throwable e) {
            throw new ElasticsearchException("Failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }
}
