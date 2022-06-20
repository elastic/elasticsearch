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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.threadpool.ExecutorBuilder;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

public class PluginsService implements ReportingService<PluginsAndModules> {

    private static final Logger logger = LogManager.getLogger(PluginsService.class);

    private final Settings settings;
    private final Path configPath;

    /**
     * We keep around a list of plugins and modules
     */
    private final List<Tuple<PluginDescriptor, Plugin>> plugins;
    private final PluginsAndModules info;

    public static final Setting<List<String>> MANDATORY_SETTING = Setting.listSetting(
        "plugin.mandatory",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public List<Setting<?>> getPluginSettings() {
        return plugins.stream().flatMap(p -> p.v2().getSettings().stream()).toList();
    }

    public List<String> getPluginSettingsFilter() {
        return plugins.stream().flatMap(p -> p.v2().getSettingsFilter().stream()).toList();
    }

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded
     */
    public PluginsService(
        Settings settings,
        Path configPath,
        Path modulesDirectory,
        Path pluginsDirectory,
        Collection<Class<? extends Plugin>> classpathPlugins
    ) {
        this.settings = settings;
        this.configPath = configPath;

        List<Tuple<PluginDescriptor, Plugin>> pluginsLoaded = new ArrayList<>();
        List<PluginDescriptor> pluginsList = new ArrayList<>();
        // we need to build a List of plugins for checking mandatory plugins
        final List<String> pluginsNames = new ArrayList<>();

        // first we load plugins that are on the classpath. this is for tests
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginDescriptor pluginDescriptor = new PluginDescriptor(
                pluginClass.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                pluginClass.getName(),
                Collections.emptyList(),
                false,
                PluginType.ISOLATED,
                "",
                false
            );
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginDescriptor);
            }
            pluginsLoaded.add(new Tuple<>(pluginDescriptor, plugin));
            pluginsList.add(pluginDescriptor);
            pluginsNames.add(pluginDescriptor.getName());
        }

        Set<Bundle> seenBundles = new LinkedHashSet<>();
        List<PluginDescriptor> modulesList = new ArrayList<>();
        // load modules
        if (modulesDirectory != null) {
            try {
                Set<Bundle> modules = getModuleBundles(modulesDirectory);
                for (Bundle bundle : modules) {
                    modulesList.add(bundle.plugin);
                }
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
                    for (final Bundle bundle : plugins) {
                        pluginsList.add(bundle.plugin);
                        pluginsNames.add(bundle.plugin.getName());
                    }
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        List<Tuple<PluginDescriptor, Plugin>> loaded = loadBundles(seenBundles);
        pluginsLoaded.addAll(loaded);

        this.info = new PluginsAndModules(pluginsList, modulesList);
        this.plugins = Collections.unmodifiableList(pluginsLoaded);

        // Checking expected plugins
        List<String> mandatoryPlugins = MANDATORY_SETTING.get(settings);
        if (mandatoryPlugins.isEmpty() == false) {
            Set<String> missingPlugins = new HashSet<>();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (pluginsNames.contains(mandatoryPlugin) == false && missingPlugins.contains(mandatoryPlugin) == false) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (missingPlugins.isEmpty() == false) {
                final String message = String.format(
                    Locale.ROOT,
                    "missing mandatory plugins [%s], found plugins [%s]",
                    Strings.collectionToDelimitedString(missingPlugins, ", "),
                    Strings.collectionToDelimitedString(pluginsNames, ", ")
                );
                throw new IllegalStateException(message);
            }
        }

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        logPluginInfo(info.getModuleInfos(), "module", logger);
        logPluginInfo(info.getPluginInfos(), "plugin", logger);
    }

    private static void logPluginInfo(final List<PluginDescriptor> pluginDescriptors, final String type, final Logger logger) {
        assert pluginDescriptors != null;
        if (pluginDescriptors.isEmpty()) {
            logger.info("no " + type + "s loaded");
        } else {
            for (final String name : pluginDescriptors.stream().map(PluginDescriptor::getName).sorted().toList()) {
                logger.info("loaded " + type + " [" + name + "]");
            }
        }
    }

    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Tuple<PluginDescriptor, Plugin> plugin : plugins) {
            Settings settings = plugin.v2().additionalSettings();
            for (String setting : settings.keySet()) {
                String oldPlugin = foundSettings.put(setting, plugin.v1().getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException(
                        "Cannot have additional setting ["
                            + setting
                            + "] "
                            + "in plugin ["
                            + plugin.v1().getName()
                            + "], already added in plugin ["
                            + oldPlugin
                            + "]"
                    );
                }
            }
            builder.put(settings);
        }
        return builder.put(this.settings).build();
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final ArrayList<ExecutorBuilder<?>> builders = new ArrayList<>();
        for (final Tuple<PluginDescriptor, Plugin> plugin : plugins) {
            builders.addAll(plugin.v2().getExecutorBuilders(settings));
        }
        return builders;
    }

    public void onIndexModule(IndexModule indexModule) {
        for (Tuple<PluginDescriptor, Plugin> plugin : plugins) {
            plugin.v2().onIndexModule(indexModule);
        }
    }

    /**
     * Get information about plugins and modules
     */
    @Override
    public PluginsAndModules info() {
        return info;
    }

    // a "bundle" is a group of jars in a single classloader
    public static class Bundle {
        public final PluginDescriptor plugin;
        public final Set<URL> urls;
        public final Set<URL> spiUrls;
        public final Set<URL> allUrls;

        public Bundle(PluginDescriptor plugin, Path dir) throws IOException {
            this.plugin = Objects.requireNonNull(plugin);

            Path spiDir = dir.resolve("spi");
            // plugin has defined an explicit api for extension
            this.spiUrls = Files.exists(spiDir) ? gatherUrls(spiDir) : null;
            this.urls = gatherUrls(dir);
            Set<URL> allUrls = new LinkedHashSet<>(urls);
            if (spiUrls != null) {
                allUrls.addAll(spiUrls);
            }
            this.allUrls = allUrls;
        }

        static Set<URL> gatherUrls(Path dir) throws IOException {
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
            return urls;
        }

        Set<URL> getExtensionUrls() {
            if (spiUrls != null) {
                return spiUrls;
            }
            return urls;
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

    /**
     * Extracts all installed plugin directories from the provided {@code rootPath}.
     *
     * @param rootPath the path where the plugins are installed
     * @return a list of all plugin paths installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the directories
     */
    public static List<Path> findPluginDirs(final Path rootPath) throws IOException {
        final List<Path> plugins = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        if (Files.exists(rootPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
                for (Path plugin : stream) {
                    final String filename = plugin.getFileName().toString();
                    if (FileSystemUtils.isDesktopServicesStore(plugin)
                        || filename.startsWith(".removing-")
                        || filename.equals(".elasticsearch-plugins.yml.cache")) {
                        continue;
                    }
                    if (seen.add(filename) == false) {
                        throw new IllegalStateException("duplicate plugin: " + plugin);
                    }
                    plugins.add(plugin);
                }
            }
        }
        return plugins;
    }

    /**
     * Verify the given plugin is compatible with the current Elasticsearch installation.
     */
    public static void verifyCompatibility(PluginDescriptor info) {
        if (info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
            throw new IllegalArgumentException(
                "Plugin ["
                    + info.getName()
                    + "] was built for Elasticsearch version "
                    + info.getElasticsearchVersion()
                    + " but version "
                    + Version.CURRENT
                    + " is running"
            );
        }
        JarHell.checkJavaVersion(info.getName(), info.getJavaVersion());
    }

    public static void checkForFailedPluginRemovals(final Path pluginsDirectory) throws IOException {
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
                    name
                );
                throw new IllegalStateException(message);
            }
        }
    }

    /** Get bundles for plugins installed in the given modules directory. */
    public static Set<Bundle> getModuleBundles(Path modulesDirectory) throws IOException {
        return findBundles(modulesDirectory, "module");
    }

    /** Get bundles for plugins installed in the given plugins directory. */
    public static Set<Bundle> getPluginBundles(final Path pluginsDirectory) throws IOException {
        return findBundles(pluginsDirectory, "plugin");
    }

    // searches subdirectories under the given directory for plugin directories
    private static Set<Bundle> findBundles(final Path directory, String type) throws IOException {
        final Set<Bundle> bundles = new HashSet<>();
        for (final Path plugin : findPluginDirs(directory)) {
            final Bundle bundle = readPluginBundle(plugin, type);
            if (bundles.add(bundle) == false) {
                throw new IllegalStateException("duplicate " + type + ": " + bundle.plugin);
            }
            if (type.equals("module") && bundle.plugin.getName().startsWith("test-") && Build.CURRENT.isSnapshot() == false) {
                throw new IllegalStateException("external test module [" + plugin.getFileName() + "] found in non-snapshot build");
            }
        }

        logger.trace(() -> "findBundles(" + type + ") returning: " + bundles.stream().map(b -> b.plugin.getName()).sorted().toList());

        return bundles;
    }

    // get a bundle for a single plugin dir
    private static Bundle readPluginBundle(final Path plugin, String type) throws IOException {
        final PluginDescriptor info;
        try {
            info = PluginDescriptor.readFromProperties(plugin);
        } catch (final IOException e) {
            throw new IllegalStateException(
                "Could not load plugin descriptor for " + type + " directory [" + plugin.getFileName() + "]",
                e
            );
        }
        return new Bundle(info, plugin);
    }

    /**
     * Return the given bundles, sorted in dependency loading order.
     *
     * This sort is stable, so that if two plugins do not have any interdependency,
     * their relative order from iteration of the provided set will not change.
     *
     * @throws IllegalStateException if a dependency cycle is found
     */
    public static List<Bundle> sortBundles(Set<Bundle> bundles) {
        Map<String, Bundle> namedBundles = bundles.stream().collect(Collectors.toMap(b -> b.plugin.getName(), Function.identity()));
        LinkedHashSet<Bundle> sortedBundles = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();
        for (Bundle bundle : bundles) {
            addSortedBundle(bundle, namedBundles, sortedBundles, dependencyStack);
        }
        return new ArrayList<>(sortedBundles);
    }

    // add the given bundle to the sorted bundles, first adding dependencies
    private static void addSortedBundle(
        Bundle bundle,
        Map<String, Bundle> bundles,
        LinkedHashSet<Bundle> sortedBundles,
        LinkedHashSet<String> dependencyStack
    ) {

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

    private List<Tuple<PluginDescriptor, Plugin>> loadBundles(Set<Bundle> bundles) {
        List<Tuple<PluginDescriptor, Plugin>> plugins = new ArrayList<>();
        Map<String, Tuple<Plugin, ClassLoader>> loaded = new HashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<Bundle> sortedBundles = sortBundles(bundles);
        for (Bundle bundle : sortedBundles) {
            if (bundle.plugin.getType() != PluginType.BOOTSTRAP) {
                checkBundleJarHell(JarHell.parseClassPath(), bundle, transitiveUrls);

                final Plugin plugin = loadBundle(bundle, loaded);
                plugins.add(new Tuple<>(bundle.plugin, plugin));
            }
        }

        loadExtensions(plugins);
        return Collections.unmodifiableList(plugins);
    }

    // package-private for test visibility
    static void loadExtensions(List<Tuple<PluginDescriptor, Plugin>> plugins) {
        Map<String, List<Plugin>> extendingPluginsByName = plugins.stream()
            .flatMap(t -> t.v1().getExtendedPlugins().stream().map(extendedPlugin -> Tuple.tuple(extendedPlugin, t.v2())))
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));
        for (Tuple<PluginDescriptor, Plugin> pluginTuple : plugins) {
            if (pluginTuple.v2() instanceof ExtensiblePlugin) {
                loadExtensionsForPlugin(
                    (ExtensiblePlugin) pluginTuple.v2(),
                    extendingPluginsByName.getOrDefault(pluginTuple.v1().getName(), List.of())
                );
            }
        }
    }

    private static void loadExtensionsForPlugin(ExtensiblePlugin extensiblePlugin, List<Plugin> extendingPlugins) {
        ExtensiblePlugin.ExtensionLoader extensionLoader = new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                List<T> result = new ArrayList<>();
                for (Plugin extendingPlugin : extendingPlugins) {
                    result.addAll(createExtensions(extensionPointType, extendingPlugin));
                }
                return Collections.unmodifiableList(result);
            }
        };

        extensiblePlugin.loadExtensions(extensionLoader);
    }

    private static <T> List<? extends T> createExtensions(Class<T> extensionPointType, Plugin plugin) {
        SPIClassIterator<T> classIterator = SPIClassIterator.get(extensionPointType, plugin.getClass().getClassLoader());
        List<T> extensions = new ArrayList<>();
        while (classIterator.hasNext()) {
            Class<? extends T> extensionClass = classIterator.next();
            extensions.add(createExtension(extensionClass, extensionPointType, plugin));
        }
        return extensions;
    }

    // package-private for test visibility
    static <T> T createExtension(Class<? extends T> extensionClass, Class<T> extensionPointType, Plugin plugin) {
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) extensionClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public " + extensionConstructorMessage(extensionClass, extensionPointType));
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public " + extensionConstructorMessage(extensionClass, extensionPointType));
        }

        final Constructor<T> constructor = constructors[0];
        if (constructor.getParameterCount() > 1) {
            throw new IllegalStateException(extensionSignatureMessage(extensionClass, extensionPointType, plugin));
        }

        if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0] != plugin.getClass()) {
            throw new IllegalStateException(
                extensionSignatureMessage(extensionClass, extensionPointType, plugin)
                    + ", not ("
                    + constructor.getParameterTypes()[0].getName()
                    + ")"
            );
        }

        try {
            if (constructor.getParameterCount() == 0) {
                return constructor.newInstance();
            } else {
                return constructor.newInstance(plugin);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                "failed to create extension [" + extensionClass.getName() + "] of type [" + extensionPointType.getName() + "]",
                e
            );
        }
    }

    private static <T> String extensionSignatureMessage(Class<? extends T> extensionClass, Class<T> extensionPointType, Plugin plugin) {
        return "signature of "
            + extensionConstructorMessage(extensionClass, extensionPointType)
            + " must be either () or ("
            + plugin.getClass().getName()
            + ")";
    }

    private static <T> String extensionConstructorMessage(Class<? extends T> extensionClass, Class<T> extensionPointType) {
        return "constructor for extension [" + extensionClass.getName() + "] of type [" + extensionPointType.getName() + "]";
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case users mess with jar files manually
    public static void checkBundleJarHell(Set<URL> classpath, Bundle bundle, Map<String, Set<URL>> transitiveUrls) {
        // invariant: any plugins this plugin bundle extends have already been added to transitiveUrls
        List<String> exts = bundle.plugin.getExtendedPlugins();

        try {
            final Logger logger = LogManager.getLogger(JarHell.class);
            Set<URL> extendedPluginUrls = new HashSet<>();
            for (String extendedPlugin : exts) {
                Set<URL> pluginUrls = transitiveUrls.get(extendedPlugin);
                assert pluginUrls != null : "transitive urls should have already been set for " + extendedPlugin;

                // consistency check: extended plugins should not have duplicate codebases with each other
                Set<URL> intersection = new HashSet<>(extendedPluginUrls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException(
                        "jar hell! extended plugins " + exts + " have duplicate codebases with each other: " + intersection
                    );
                }

                // jar hell check: extended plugins (so far) do not have jar hell with each other
                extendedPluginUrls.addAll(pluginUrls);
                JarHell.checkJarHell(extendedPluginUrls, logger::debug);

                // consistency check: each extended plugin should not have duplicate codebases with implementation+spi of this plugin
                intersection = new HashSet<>(bundle.allUrls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException(
                        "jar hell! duplicate codebases with extended plugin [" + extendedPlugin + "]: " + intersection
                    );
                }

                // jar hell check: extended plugins (so far) do not have jar hell with implementation+spi of this plugin
                Set<URL> implementation = new HashSet<>(bundle.allUrls);
                implementation.addAll(extendedPluginUrls);
                JarHell.checkJarHell(implementation, logger::debug);
            }

            // Set transitive urls for other plugins to extend this plugin. Note that jarhell has already been checked above.
            // This uses the extension urls (spi if set) since the implementation will not be in the transitive classpath at runtime.
            extendedPluginUrls.addAll(bundle.getExtensionUrls());
            transitiveUrls.put(bundle.plugin.getName(), extendedPluginUrls);

            // check we don't have conflicting codebases with core
            Set<URL> intersection = new HashSet<>(classpath);
            intersection.retainAll(bundle.allUrls);
            if (intersection.isEmpty() == false) {
                throw new IllegalStateException("jar hell! duplicate codebases between plugin and core: " + intersection);
            }
            // check we don't have conflicting classes
            Set<URL> union = new HashSet<>(classpath);
            union.addAll(bundle.allUrls);
            JarHell.checkJarHell(union, logger::debug);
        } catch (final IllegalStateException ise) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " due to jar hell", ise);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " while checking for jar hell", e);
        }
    }

    private Plugin loadBundle(Bundle bundle, Map<String, Tuple<Plugin, ClassLoader>> loaded) {
        String name = bundle.plugin.getName();

        verifyCompatibility(bundle.plugin);

        // collect loaders of extended plugins
        List<ClassLoader> extendedLoaders = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            Tuple<Plugin, ClassLoader> extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isInstance(extendedPlugin.v1()) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            extendedLoaders.add(extendedPlugin.v2());
        }

        ClassLoader parentLoader = PluginLoaderIndirection.createLoader(getClass().getClassLoader(), extendedLoaders);
        ClassLoader spiLoader = null;
        if (bundle.spiUrls != null) {
            spiLoader = URLClassLoader.newInstance(bundle.spiUrls.toArray(new URL[0]), parentLoader);
        }

        ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), spiLoader == null ? parentLoader : spiLoader);
        if (spiLoader == null) {
            // use full implementation for plugins extending this one
            spiLoader = loader;
        }

        // reload SPI with any new services from the plugin
        reloadLuceneSPI(loader);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            // Set context class loader to plugin's class loader so that plugins
            // that have dependencies with their own SPI endpoints have a chance to load
            // and initialize them appropriately.
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                Thread.currentThread().setContextClassLoader(loader);
                return null;
            });

            Class<? extends Plugin> pluginClass = loadPluginClass(bundle.plugin.getClassname(), loader);
            if (loader != pluginClass.getClassLoader()) {
                throw new IllegalStateException(
                    "Plugin ["
                        + name
                        + "] must reference a class loader local Plugin class ["
                        + bundle.plugin.getClassname()
                        + "] (class loader ["
                        + pluginClass.getClassLoader()
                        + "])"
                );
            }
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            loaded.put(name, Tuple.tuple(plugin, spiLoader));
            return plugin;
        } finally {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                Thread.currentThread().setContextClassLoader(cl);
                return null;
            });
        }
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
    }

    private static Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) {
        try {
            return Class.forName(className, false, loader).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new ElasticsearchException("Could not find plugin class [" + className + "]", e);
        }
    }

    private static Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings, Path configPath) {
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

        final Class<?>[] parameterTypes = constructor.getParameterTypes();
        try {
            if (constructor.getParameterCount() == 2 && parameterTypes[0] == Settings.class && parameterTypes[1] == Path.class) {
                return (Plugin) constructor.newInstance(settings, configPath);
            } else if (constructor.getParameterCount() == 1 && parameterTypes[0] == Settings.class) {
                return (Plugin) constructor.newInstance(settings);
            } else if (constructor.getParameterCount() == 0) {
                return (Plugin) constructor.newInstance();
            } else {
                throw new IllegalStateException(signatureMessage(pluginClass));
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private static String signatureMessage(final Class<? extends Plugin> clazz) {
        return String.format(
            Locale.ROOT,
            "no public constructor of correct signature for [%s]; must be [%s], [%s], or [%s]",
            clazz.getName(),
            "(org.elasticsearch.common.settings.Settings,java.nio.file.Path)",
            "(org.elasticsearch.common.settings.Settings)",
            "()"
        );
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.v2().getClass())).map(p -> ((T) p.v2())).toList();
    }
}
