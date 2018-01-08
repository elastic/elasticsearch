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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public final class PluginsService {

    /**
     * We keep around a list of plugins and modules
     */
    private final List<Tuple<PluginInfo, Plugin>> plugins;
    private final PluginsAndModules info;
    public static final Setting<List<String>> MANDATORY_SETTING =
        Setting.listSetting("plugin.mandatory", Collections.emptyList(), Function.identity(), Property.NodeScope);

    private final PluginsAndModules pluginsAndModules;
    List<Tuple<PluginInfo, Class<? extends Plugin>>> pluginClasses;
    private final Settings settings;
    private Map<PluginInfo, Plugin.PluginSettings> pluginSettings = new HashMap<>();

    /**
     * Constructs a new PluginService
     * @param nodeSettings The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public PluginsService(Settings nodeSettings, Path modulesDirectory, Path pluginsDirectory, Path configPath,
                      Collection<Class<? extends Plugin>> classpathPlugins) {
        pluginClasses = new ArrayList<>();
        List<PluginInfo> pluginsList = new ArrayList<>();
        List<PluginInfo> modulesList = new ArrayList<>();
        // first we load plugins that are on the classpath. this is for tests and transport clients
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            PluginInfo pluginInfo = new PluginInfo(pluginClass.getName(), "classpath plugin", "NA",
                pluginClass.getName(), Collections.emptyList(), false, false);
            pluginClasses.add(new Tuple<>(pluginInfo, pluginClass));
        }
        Logger logger = Loggers.getLogger(PluginsService.class, nodeSettings);
        Set<Bundle> seenBundles = loadBundles(logger, modulesDirectory, pluginsDirectory);
        pluginClasses.addAll(loadBundleClasses(seenBundles));
        for (Bundle bundle : seenBundles) {
            if (bundle.isModule) {
                modulesList.add(bundle.plugin);
            } else {
                pluginsList.add(bundle.plugin);
            }
        }
        pluginsAndModules = new PluginsAndModules(pluginsList, modulesList);
        for (Tuple<PluginInfo, Class<? extends Plugin>> pluginClass : pluginClasses) {
            Plugin.PluginSettings loaded = load(pluginClass.v2(), nodeSettings);
            if (loaded != null) {
                pluginSettings.put(pluginClass.v1(), loaded);
            }
        }
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Map.Entry<PluginInfo, Plugin.PluginSettings> loaded  : pluginSettings.entrySet()) {
            PluginInfo info = loaded.getKey();
            Plugin.PluginSettings pluginSettings = loaded.getValue();
            Settings settings = pluginSettings.getSettings();
            for (String setting : settings.keySet()) {
                String oldPlugin = foundSettings.put(setting, info.getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException("Cannot have additional setting [" + setting + "] " +
                        "in plugin [" + info.getName() + "], already added in plugin [" + oldPlugin + "]");
                }
            }
            builder.put(settings);
        }
        settings = builder.put(nodeSettings).build();
        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = new ArrayList<>();
        Map<String, Plugin> extensiblePluginMap = new HashMap<>();
        for (Tuple<PluginInfo, Class<? extends Plugin>> plugin : pluginClasses) {
            Plugin instance = loadPlugin(plugin.v2(), settings, configPath);
            extensiblePluginMap.put(plugin.v1().getName(), instance);
            pluginsLoaded.add(new Tuple<>(plugin.v1(), instance));
        }
        for (Tuple<PluginInfo, Class<? extends Plugin>> plugin : pluginClasses) {
            ClassLoader loader = extensiblePluginMap.get(plugin.v1().getName()).getClass().getClassLoader();
            // reload SPI with any new services from the plugin
            reloadLuceneSPI(loader);
            for (String extendedPluginName : plugin.v1().getExtendedPlugins()) {
                ExtensiblePlugin.class.cast(extensiblePluginMap.get(extendedPluginName)).reloadSPI(loader);
            }
        }
        this.info = pluginsAndModules;
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

    private static Plugin.PluginSettings load(Class<? extends Plugin> pluginClass, Settings nodeSettings) {
        try {
            Method getPluginSettings = pluginClass.getMethod("getPluginSettings", Settings.class);
            if ((getPluginSettings.getModifiers() | Modifier.STATIC) == 0) {
                throw new IllegalStateException("getPluginSettings must be static");
            }
            if ((getPluginSettings.getModifiers() | Modifier.PUBLIC) == 0) {
                throw new IllegalStateException("getPluginSettings must be public");
            }
            Plugin.PluginSettings invoke = (Plugin.PluginSettings) getPluginSettings.invoke(pluginClass, nodeSettings);
            return invoke;
        } catch (NoSuchMethodException e) {
            // that's fine this plugin doesn't have any settings to extend
            return null;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("can't load plugin settings", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("can't load plugin settings", e);
        }
    }

    private static Set<Bundle> loadBundles(Logger logger, Path modulesDirectory, Path pluginsDirectory) {
        Set<Bundle> seenBundles = new LinkedHashSet<>();
        // load modules
        if (modulesDirectory != null) {
            try {
                Set<Bundle> modules = getModuleBundles(modulesDirectory);
                seenBundles.addAll(modules);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // now, find all the ones that are in plugins/
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    checkForFailedPluginRemovals(pluginsDirectory);
                    Set<Bundle> plugins = getPluginBundles(pluginsDirectory);
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }
        return seenBundles;
    }

    /**
     * Returns all plugin declared settings
     */
    public List<Setting<?>> getDeclaredSettings() {
        List<Setting<?>> declaredSettings = new ArrayList<>();
        for (Map.Entry<PluginInfo, Plugin.PluginSettings> loaded : pluginSettings.entrySet()) {
            Plugin.PluginSettings pluginSettings = loaded.getValue();
            declaredSettings.addAll(pluginSettings.getDeclaredSettings());
        }
        return declaredSettings;
    }

    /**
     * Returns all plugin declared settings filter
     * @return
     */
    public Set<String> getPluginSettingsFilter() {
        Set<String> pluginSettingsFilter = new HashSet<>();
        for (Map.Entry<PluginInfo, Plugin.PluginSettings> loaded : pluginSettings.entrySet()) {
            Plugin.PluginSettings pluginSettings = loaded.getValue();
            pluginSettingsFilter.addAll(pluginSettings.getSettingsFilter());
        }
        return pluginSettingsFilter;
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

    public Settings getSettings() {
        return settings;
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
        final PluginInfo plugin;
        final Set<URL> urls;
        final boolean isModule;

        Bundle(PluginInfo plugin, Path dir, boolean isModule) throws IOException {
            this.isModule = isModule;
            this.plugin = Objects.requireNonNull(plugin);
            Set<URL> urls = new LinkedHashSet<>();
            // gather urls for jar files
            try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(dir, "*.jar")) {
                for (Path jar : jarStream) {
                    // normalize with toRealPath to get symlinks out of our hair
                    URL url = jar.toRealPath().toUri().toURL();
                    if (urls.add(url) == false) {
                        throw new IllegalStateException("duplicate codebase: " + url);
                    }
                }
            }
            this.urls = Objects.requireNonNull(urls);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bundle bundle = (Bundle) o;
            return Objects.equals(plugin, bundle.plugin);
        }

        @Override
        public int hashCode() {
            return Objects.hash(plugin);
        }
    }

    // similar in impl to getPluginBundles, but DO NOT try to make them share code.
    // we don't need to inherit all the leniency, and things are different enough.
    static Set<Bundle> getModuleBundles(Path modulesDirectory) throws IOException {
        // damn leniency
        if (Files.notExists(modulesDirectory)) {
            return Collections.emptySet();
        }
        Set<Bundle> bundles = new LinkedHashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(modulesDirectory)) {
            for (Path module : stream) {
                PluginInfo info = PluginInfo.readFromProperties(module);
                if (bundles.add(new Bundle(info, module, true)) == false) {
                    throw new IllegalStateException("duplicate module: " + info);
                }
            }
        }
        return bundles;
    }

    static void checkForFailedPluginRemovals(final Path pluginsDirectory) throws IOException {
        /*
         * Check for the existence of a marker file that indicates any plugins are in a garbage state from a failed attempt to remove the
         * plugin.
         */
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory, ".removing-*")) {
            final Iterator<Path> iterator = stream.iterator();
            if (iterator.hasNext()) {
                final Path removing = iterator.next();
                final String fileName = removing.getFileName().toString();
                final String name = fileName.substring(1 + fileName.indexOf("-"));
                final String message = String.format(
                        Locale.ROOT,
                        "found file [%s] from a failed attempt to remove the plugin [%s]; execute [elasticsearch-plugin remove %2$s]",
                        removing,
                        name);
                throw new IllegalStateException(message);
            }
        }
    }

    static Set<Bundle> getPluginBundles(Path pluginsDirectory) throws IOException {
        Logger logger = Loggers.getLogger(PluginsService.class);
        Set<Bundle> bundles = new LinkedHashSet<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory)) {
            for (Path plugin : stream) {
                if (FileSystemUtils.isDesktopServicesStore(plugin)) {
                    continue;
                }
                if (plugin.getFileName().toString().startsWith(".removing-")) {
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

                if (bundles.add(new Bundle(info, plugin, false)) == false) {
                    throw new IllegalStateException("duplicate plugin: " + info);
                }
            }
        }

        return bundles;
    }

    /**
     * Return the given bundles, sorted in dependency loading order.
     *
     * This sort is stable, so that if two plugins do not have any interdependency,
     * their relative order from iteration of the provided set will not change.
     *
     * @throws IllegalStateException if a dependency cycle is found
     */
    // pkg private for tests
    static List<Bundle> sortBundles(Set<Bundle> bundles) {
        Map<String, Bundle> namedBundles = bundles.stream().collect(Collectors.toMap(b -> b.plugin.getName(), Function.identity()));
        LinkedHashSet<Bundle> sortedBundles = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();
        for (Bundle bundle : bundles) {
            addSortedBundle(bundle, namedBundles, sortedBundles, dependencyStack);
        }
        return new ArrayList<>(sortedBundles);
    }

    // add the given bundle to the sorted bundles, first adding dependencies
    private static void addSortedBundle(Bundle bundle, Map<String, Bundle> bundles, LinkedHashSet<Bundle> sortedBundles,
                                        LinkedHashSet<String> dependencyStack) {

        String name = bundle.plugin.getName();
        if (dependencyStack.contains(name)) {
            StringBuilder msg = new StringBuilder("Cycle found in plugin dependencies: ");
            dependencyStack.forEach(s -> {
                msg.append(s);
                msg.append(" -> ");
            });
            msg.append(name);
            throw new IllegalStateException(msg.toString());
        }
        if (sortedBundles.contains(bundle)) {
            // already added this plugin, via a dependency
            return;
        }

        dependencyStack.add(name);
        for (String dependency : bundle.plugin.getExtendedPlugins()) {
            Bundle depBundle = bundles.get(dependency);
            if (depBundle == null) {
                throw new IllegalArgumentException("Missing plugin [" + dependency + "], dependency of [" + name + "]");
            }
            addSortedBundle(depBundle, bundles, sortedBundles, dependencyStack);
            assert sortedBundles.contains(depBundle);
        }
        dependencyStack.remove(name);

        sortedBundles.add(bundle);
    }


    static List<Tuple<PluginInfo, Class<? extends Plugin>>> loadBundleClasses(Set<Bundle> bundles) {
        List<Tuple<PluginInfo, Class<? extends Plugin>>> plugins = new ArrayList<>();
        Map<String, Class<? extends Plugin>> loaded = new HashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<Bundle> sortedBundles = sortBundles(bundles);

        for (Bundle bundle : sortedBundles) {
            checkBundleJarHell(bundle, transitiveUrls);
            final Class<? extends Plugin> pluginClass = loadBundle(bundle, loaded);
            plugins.add(new Tuple<>(bundle.plugin, pluginClass));
        }
        return Collections.unmodifiableList(plugins);
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case lusers mess with jar files manually
    static void checkBundleJarHell(Bundle bundle, Map<String, Set<URL>> transitiveUrls) {
        // invariant: any plugins this plugin bundle extends have already been added to transitiveUrls
        List<String> exts = bundle.plugin.getExtendedPlugins();

        try {
            Set<URL> urls = new HashSet<>();
            for (String extendedPlugin : exts) {
                Set<URL> pluginUrls = transitiveUrls.get(extendedPlugin);
                assert pluginUrls != null : "transitive urls should have already been set for " + extendedPlugin;

                Set<URL> intersection = new HashSet<>(urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException("jar hell! extended plugins " + exts +
                                                    " have duplicate codebases with each other: " + intersection);
                }

                intersection = new HashSet<>(bundle.urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException("jar hell! duplicate codebases with extended plugin [" +
                                                    extendedPlugin + "]: " + intersection);
                }

                urls.addAll(pluginUrls);
                JarHell.checkJarHell(urls); // check jarhell as we add each extended plugin's urls
            }

            urls.addAll(bundle.urls);
            JarHell.checkJarHell(urls); // check jarhell of each extended plugin against this plugin
            transitiveUrls.put(bundle.plugin.getName(), urls);

            Set<URL> classpath = JarHell.parseClassPath();
            // check we don't have conflicting codebases with core
            Set<URL> intersection = new HashSet<>(classpath);
            intersection.retainAll(bundle.urls);
            if (intersection.isEmpty() == false) {
                throw new IllegalStateException("jar hell! duplicate codebases between plugin and core: " + intersection);
            }
            // check we don't have conflicting classes
            Set<URL> union = new HashSet<>(classpath);
            union.addAll(bundle.urls);
            JarHell.checkJarHell(union);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " due to jar hell", e);
        }
    }

    private static Class<? extends Plugin> loadBundle(Bundle bundle, Map<String, Class<? extends Plugin>> loaded) {
        String name = bundle.plugin.getName();

        // collect loaders of extended plugins
        List<ClassLoader> extendedLoaders = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            Class<? extends Plugin> extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isAssignableFrom(extendedPlugin) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            extendedLoaders.add(extendedPlugin.getClassLoader());
        }

        // create a child to load the plugin in this bundle
        ClassLoader parentLoader = PluginLoaderIndirection.createLoader(PluginsService.class.getClassLoader(), extendedLoaders);
        ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), parentLoader);

        Class<? extends Plugin> pluginClass = loadPluginClass(bundle.plugin.getClassname(), loader);
        loaded.put(name, pluginClass);

        return pluginClass;
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

    private static Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new ElasticsearchException("Could not find plugin class [" + className + "]", e);
        }
    }

    private Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings, Path configPath) {
        // TODO instead of config path we should pass down Environment since we now have a fully setup env that we can use.
        // this should be a follow-up change
        final Constructor<?>[] constructors = pluginClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + pluginClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + pluginClass.getName() + "]");
        }

        final Constructor<?> constructor = constructors[0];
        if (constructor.getParameterCount() > 2) {
            throw new IllegalStateException(signatureMessage(pluginClass));
        }

        final Class[] parameterTypes = constructor.getParameterTypes();
        try {
            if (constructor.getParameterCount() == 2 && parameterTypes[0] == Settings.class && parameterTypes[1] == Path.class) {
                return (Plugin)constructor.newInstance(settings, configPath);
            } else if (constructor.getParameterCount() == 1 && parameterTypes[0] == Settings.class) {
                return (Plugin)constructor.newInstance(settings);
            } else if (constructor.getParameterCount() == 0) {
                return (Plugin)constructor.newInstance();
            } else {
                throw new IllegalStateException(signatureMessage(pluginClass));
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private String signatureMessage(final Class<? extends Plugin> clazz) {
        return String.format(
                Locale.ROOT,
                "no public constructor of correct signature for [%s]; must be [%s], [%s], or [%s]",
                clazz.getName(),
                "(org.elasticsearch.common.settings.Settings,java.nio.file.Path)",
                "(org.elasticsearch.common.settings.Settings)",
                "()");
    }

    public <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.v2().getClass()))
            .map(p -> ((T)p.v2())).collect(Collectors.toList());
    }
}
