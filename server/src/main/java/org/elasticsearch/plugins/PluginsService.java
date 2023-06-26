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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.plugins.spi.SPIClassIterator;

import java.io.IOException;
import java.lang.ModuleLayer.Controller;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginsService implements ReportingService<PluginsAndModules> {

    public StablePluginsRegistry getStablePluginRegistry() {
        return stablePluginsRegistry;
    }

    /**
     * A loaded plugin is one for which Elasticsearch has successfully constructed an instance of the plugin's class
     * @param descriptor Metadata about the plugin, usually loaded from plugin properties
     * @param instance The constructed instance of the plugin's main class
     * @param loader   The classloader for the plugin
     * @param layer   The module layer for the plugin
     */
    record LoadedPlugin(PluginDescriptor descriptor, Plugin instance, ClassLoader loader, ModuleLayer layer) {

        LoadedPlugin {
            Objects.requireNonNull(descriptor);
            Objects.requireNonNull(instance);
            Objects.requireNonNull(loader);
            Objects.requireNonNull(layer);
        }

        /**
         * Creates a loaded <i>classpath plugin</i>. A <i>classpath plugin</i> is a plugin loaded
         * by the system classloader and defined to the unnamed module of the boot layer.
         */
        LoadedPlugin(PluginDescriptor descriptor, Plugin instance) {
            this(descriptor, instance, PluginsService.class.getClassLoader(), ModuleLayer.boot());
        }
    }

    private static final Logger logger = LogManager.getLogger(PluginsService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(PluginsService.class);

    private static final Map<String, List<ModuleQualifiedExportsService>> exportsServices;

    static {
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports = new HashMap<>();
        var loader = ServiceLoader.load(ModuleQualifiedExportsService.class, PluginsService.class.getClassLoader());
        for (var exportsService : loader) {
            addExportsService(qualifiedExports, exportsService, exportsService.getClass().getModule().getName());
        }
        exportsServices = Map.copyOf(qualifiedExports);
    }

    private final Settings settings;
    private final Path configPath;

    /**
     * We keep around a list of plugins and modules. The order of
     * this list is that which the plugins and modules were loaded in.
     */
    private final List<LoadedPlugin> plugins;
    private final PluginsAndModules info;
    private final StablePluginsRegistry stablePluginsRegistry = new StablePluginsRegistry();

    public static final Setting<List<String>> MANDATORY_SETTING = Setting.stringListSetting("plugin.mandatory", Property.NodeScope);

    /**
     * Constructs a new PluginService
     *
     * @param settings         The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     */
    public PluginsService(Settings settings, Path configPath, Path modulesDirectory, Path pluginsDirectory) {
        this.settings = settings;
        this.configPath = configPath;

        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports = new HashMap<>(exportsServices);
        addServerExportsService(qualifiedExports);

        Set<PluginBundle> seenBundles = new LinkedHashSet<>();

        // load modules
        List<PluginDescriptor> modulesList = new ArrayList<>();
        Set<String> moduleNameList = new HashSet<>();
        if (modulesDirectory != null) {
            try {
                Set<PluginBundle> modules = PluginsUtils.getModuleBundles(modulesDirectory);
                modules.stream().map(PluginBundle::pluginDescriptor).forEach(m -> {
                    modulesList.add(m);
                    moduleNameList.add(m.getName());
                });
                seenBundles.addAll(modules);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // load plugins
        List<PluginDescriptor> pluginsList = new ArrayList<>();
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    PluginsUtils.checkForFailedPluginRemovals(pluginsDirectory);
                    Set<PluginBundle> plugins = PluginsUtils.getPluginBundles(pluginsDirectory);
                    plugins.stream().map(PluginBundle::pluginDescriptor).forEach(pluginsList::add);
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        LinkedHashMap<String, LoadedPlugin> loadedPlugins = loadBundles(seenBundles, qualifiedExports);

        var inspector = PluginIntrospector.getInstance();
        this.info = new PluginsAndModules(getRuntimeInfos(inspector, pluginsList, loadedPlugins), modulesList);
        this.plugins = List.copyOf(loadedPlugins.values());

        checkDeprecations(inspector, pluginsList, loadedPlugins);

        checkMandatoryPlugins(
            pluginsList.stream().map(PluginDescriptor::getName).collect(Collectors.toSet()),
            new HashSet<>(MANDATORY_SETTING.get(settings))
        );

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        for (String name : loadedPlugins.keySet()) {
            if (moduleNameList.contains(name)) {
                logger.info("loaded module [{}]", name);
            } else {
                logger.info("loaded plugin [{}]", name);
            }
        }
    }

    // package-private for testing
    static void checkMandatoryPlugins(Set<String> existingPlugins, Set<String> mandatoryPlugins) {
        if (mandatoryPlugins.isEmpty()) {
            return;
        }

        Set<String> missingPlugins = Sets.difference(mandatoryPlugins, existingPlugins);
        if (missingPlugins.isEmpty() == false) {
            final String message = "missing mandatory plugins ["
                + String.join(", ", missingPlugins)
                + "], found plugins ["
                + String.join(", ", existingPlugins)
                + "]";
            throw new IllegalStateException(message);
        }
    }

    private static final Set<String> officialPlugins;

    static {
        try (var stream = PluginsService.class.getResourceAsStream("/plugins.txt")) {
            officialPlugins = Streams.readAllLines(stream).stream().map(String::trim).collect(Collectors.toUnmodifiableSet());
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    private static List<PluginRuntimeInfo> getRuntimeInfos(
        PluginIntrospector inspector,
        List<PluginDescriptor> pluginDescriptors,
        Map<String, LoadedPlugin> plugins
    ) {
        List<PluginRuntimeInfo> runtimeInfos = new ArrayList<>();
        for (PluginDescriptor descriptor : pluginDescriptors) {
            LoadedPlugin plugin = plugins.get(descriptor.getName());
            assert plugin != null;
            Class<?> pluginClazz = plugin.instance.getClass();
            boolean isOfficial = officialPlugins.contains(descriptor.getName());
            PluginApiInfo apiInfo = null;
            if (isOfficial == false) {
                apiInfo = new PluginApiInfo(inspector.interfaces(pluginClazz), inspector.overriddenMethods(pluginClazz));
            }
            runtimeInfos.add(new PluginRuntimeInfo(descriptor, isOfficial, apiInfo));
        }
        return runtimeInfos;
    }

    /**
     * Map a function over all plugins
     * @param function a function that takes a plugin and returns a result
     * @return A stream of results
     * @param <T> The generic type of the result
     */
    public final <T> Stream<T> map(Function<Plugin, T> function) {
        return plugins().stream().map(LoadedPlugin::instance).map(function);
    }

    /**
     * FlatMap a function over all plugins
     * @param function a function that takes a plugin and returns a collection
     * @return A stream of results
     * @param <T> The generic type of the collection
     */
    public final <T> Stream<T> flatMap(Function<Plugin, Collection<T>> function) {
        return plugins().stream().map(LoadedPlugin::instance).flatMap(p -> function.apply(p).stream());
    }

    /**
     * Apply a consumer action to each plugin
     * @param consumer An action that consumes a plugin
     */
    public final void forEach(Consumer<Plugin> consumer) {
        plugins().stream().map(LoadedPlugin::instance).forEach(consumer);
    }

    /**
     * Sometimes we want the plugin name for error handling.
     * @return A map of plugin names to plugin instances.
     */
    public final Map<String, Plugin> pluginMap() {
        return plugins().stream().collect(Collectors.toMap(p -> p.descriptor().getName(), LoadedPlugin::instance));
    }

    /**
     * Get information about plugins and modules
     */
    @Override
    public PluginsAndModules info() {
        return info;
    }

    protected List<LoadedPlugin> plugins() {
        return this.plugins;
    }

    private LinkedHashMap<String, LoadedPlugin> loadBundles(
        Set<PluginBundle> bundles,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        LinkedHashMap<String, LoadedPlugin> loaded = new LinkedHashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        if (sortedBundles.isEmpty() == false) {
            Set<URL> systemLoaderURLs = JarHell.parseModulesAndClassPath();
            for (PluginBundle bundle : sortedBundles) {
                PluginsUtils.checkBundleJarHell(systemLoaderURLs, bundle, transitiveUrls);
                loadBundle(bundle, loaded, qualifiedExports);
            }
        }

        loadExtensions(loaded.values());
        return loaded;
    }

    // package-private for test visibility
    static void loadExtensions(Collection<LoadedPlugin> plugins) {
        Map<String, List<Plugin>> extendingPluginsByName = plugins.stream()
            .flatMap(t -> t.descriptor().getExtendedPlugins().stream().map(extendedPlugin -> Tuple.tuple(extendedPlugin, t.instance())))
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));
        for (LoadedPlugin pluginTuple : plugins) {
            if (pluginTuple.instance() instanceof ExtensiblePlugin) {
                loadExtensionsForPlugin(
                    (ExtensiblePlugin) pluginTuple.instance(),
                    extendingPluginsByName.getOrDefault(pluginTuple.descriptor().getName(), List.of())
                );
            }
        }
    }

    /**
     * SPI convenience method that uses the {@link ServiceLoader} JDK class to load various SPI providers
     * from plugins/modules.
     * <p>
     * For example:
     *
     * <pre>
     * var pluginHandlers = pluginsService.loadServiceProviders(OperatorHandlerProvider.class);
     * </pre>
     * @param service A templated service class to look for providers in plugins
     * @return an immutable {@link List} of discovered providers in the plugins/modules
     */
    public <T> List<? extends T> loadServiceProviders(Class<T> service) {
        List<T> result = new ArrayList<>();

        for (LoadedPlugin pluginTuple : plugins()) {
            result.addAll(createExtensions(service, pluginTuple.instance));
        }

        return Collections.unmodifiableList(result);
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

        Constructor<T> constructor = constructors[0];
        // Using modules and SPI requires that we declare the default no-arg constructor apart from our custom
        // one arg constructor with a plugin.
        if (constructors.length == 2) {
            // we prefer the one arg constructor in this case
            if (constructors[1].getParameterCount() > 0) {
                constructor = constructors[1];
            }
        } else if (constructors.length > 1) {
            throw new IllegalStateException("no unique public " + extensionConstructorMessage(extensionClass, extensionPointType));
        }

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

    private void loadBundle(
        PluginBundle bundle,
        Map<String, LoadedPlugin> loaded,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        String name = bundle.plugin.getName();
        logger.debug(() -> "Loading bundle: " + name);

        PluginsUtils.verifyCompatibility(bundle.plugin);

        // collect the list of extended plugins
        List<LoadedPlugin> extendedPlugins = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            LoadedPlugin extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isInstance(extendedPlugin.instance()) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            assert extendedPlugin.loader() != null : "All non-classpath plugins should be loaded with a classloader";
            extendedPlugins.add(extendedPlugin);
            logger.debug(
                () -> "Loading bundle: " + name + ", ext plugins: " + extendedPlugins.stream().map(lp -> lp.descriptor().getName()).toList()
            );
        }

        final ClassLoader parentLoader = PluginLoaderIndirection.createLoader(
            getClass().getClassLoader(),
            extendedPlugins.stream().map(LoadedPlugin::loader).toList()
        );
        LayerAndLoader spiLayerAndLoader = null;
        if (bundle.hasSPI()) {
            spiLayerAndLoader = createSPI(bundle, parentLoader, extendedPlugins, qualifiedExports);
        }

        final ClassLoader pluginParentLoader = spiLayerAndLoader == null ? parentLoader : spiLayerAndLoader.loader();
        final LayerAndLoader pluginLayerAndLoader = createPlugin(
            bundle,
            pluginParentLoader,
            extendedPlugins,
            spiLayerAndLoader,
            qualifiedExports
        );
        final ClassLoader pluginClassLoader = pluginLayerAndLoader.loader();

        if (spiLayerAndLoader == null) {
            // use full implementation for plugins extending this one
            spiLayerAndLoader = pluginLayerAndLoader;
        }

        // reload SPI with any new services from the plugin
        reloadLuceneSPI(pluginClassLoader);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            // Set context class loader to plugin's class loader so that plugins
            // that have dependencies with their own SPI endpoints have a chance to load
            // and initialize them appropriately.
            privilegedSetContextClassLoader(pluginClassLoader);

            Plugin plugin;
            if (bundle.pluginDescriptor().isStable()) {
                stablePluginsRegistry.scanBundleForStablePlugins(bundle, pluginClassLoader);
                /*
                Contrary to old plugins we don't need an instance of the plugin here.
                Stable plugin register components (like CharFilterFactory) in stable plugin registry, which is then used in AnalysisModule
                when registering char filter factories and other analysis components.
                We don't have to support for settings, additional components and other methods
                that are in org.elasticsearch.plugins.Plugin
                We need to pass a name though so that we can show that a plugin was loaded (via cluster state api)
                This might need to be revisited once support for settings is added
                 */
                plugin = new StablePluginPlaceHolder(bundle.plugin.getName());
            } else {

                Class<? extends Plugin> pluginClass = loadPluginClass(bundle.plugin.getClassname(), pluginClassLoader);
                if (pluginClassLoader != pluginClass.getClassLoader()) {
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
                plugin = loadPlugin(pluginClass, settings, configPath);
            }
            loaded.put(name, new LoadedPlugin(bundle.plugin, plugin, spiLayerAndLoader.loader(), spiLayerAndLoader.layer()));
        } finally {
            privilegedSetContextClassLoader(cl);
        }
    }

    static LayerAndLoader createSPI(
        PluginBundle bundle,
        ClassLoader parentLoader,
        List<LoadedPlugin> extendedPlugins,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        final PluginDescriptor plugin = bundle.plugin;
        if (plugin.getModuleName().isPresent()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", creating spi, modular");
            return createSpiModuleLayer(
                bundle.spiUrls,
                parentLoader,
                extendedPlugins.stream().map(LoadedPlugin::layer).toList(),
                qualifiedExports
            );
        } else {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", creating spi, non-modular");
            return LayerAndLoader.ofLoader(URLClassLoader.newInstance(bundle.spiUrls.toArray(new URL[0]), parentLoader));
        }
    }

    static LayerAndLoader createPlugin(
        PluginBundle bundle,
        ClassLoader pluginParentLoader,
        List<LoadedPlugin> extendedPlugins,
        LayerAndLoader spiLayerAndLoader,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        final PluginDescriptor plugin = bundle.plugin;
        if (plugin.getModuleName().isPresent()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", modular");
            var parentLayers = Stream.concat(
                Stream.ofNullable(spiLayerAndLoader != null ? spiLayerAndLoader.layer() : null),
                extendedPlugins.stream().map(LoadedPlugin::layer)
            ).toList();
            return createPluginModuleLayer(bundle, pluginParentLoader, parentLayers, qualifiedExports);
        } else if (plugin.isStable()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", non-modular as synthetic module");
            return LayerAndLoader.ofLoader(
                UberModuleClassLoader.getInstance(
                    pluginParentLoader,
                    ModuleLayer.boot(),
                    "synthetic." + toModuleName(plugin.getName()),
                    bundle.allUrls,
                    Set.of("org.elasticsearch.server") // TODO: instead of denying server, allow only jvm + stable API modules
                )
            );
        } else {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", non-modular");
            return LayerAndLoader.ofLoader(URLClassLoader.newInstance(bundle.urls.toArray(URL[]::new), pluginParentLoader));
        }
    }

    // package-visible for testing
    static String toModuleName(String name) {
        String result = name.replaceAll("\\W+", ".") // replace non-alphanumeric character strings with dots
            .replaceAll("(^[^A-Za-z_]*)", "") // trim non-alpha or underscore characters from start
            .replaceAll("\\.$", "") // trim trailing dot
            .toLowerCase(Locale.getDefault());
        assert ModuleSupport.isPackageName(result);
        return result;
    }

    private static void checkDeprecations(
        PluginIntrospector inspector,
        List<PluginDescriptor> pluginDescriptors,
        Map<String, LoadedPlugin> plugins
    ) {
        for (PluginDescriptor descriptor : pluginDescriptors) {
            LoadedPlugin plugin = plugins.get(descriptor.getName());
            Class<?> pluginClazz = plugin.instance.getClass();
            for (String deprecatedInterface : inspector.deprecatedInterfaces(pluginClazz)) {
                deprecationLogger.warn(
                    DeprecationCategory.PLUGINS,
                    pluginClazz.getName() + deprecatedInterface,
                    "Plugin class {} from plugin {} implements deprecated plugin interface {}. "
                        + "This plugin interface will be removed in a future release.",
                    pluginClazz.getName(),
                    descriptor.getName(),
                    deprecatedInterface
                );
            }
            for (var deprecatedMethodInInterface : inspector.deprecatedMethods(pluginClazz).entrySet()) {
                String methodName = deprecatedMethodInInterface.getKey();
                String interfaceName = deprecatedMethodInInterface.getValue();
                deprecationLogger.warn(
                    DeprecationCategory.PLUGINS,
                    pluginClazz.getName() + methodName + interfaceName,
                    "Plugin class {} from plugin {} implements deprecated method {} from plugin interface {}. "
                        + "This method will be removed in a future release.",
                    pluginClazz.getName(),
                    descriptor.getName(),
                    methodName,
                    interfaceName
                );
            }
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

    // package-private for testing
    static Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings, Path configPath) {
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
    public final <T> List<T> filterPlugins(Class<T> type) {
        return plugins().stream().filter(x -> type.isAssignableFrom(x.instance().getClass())).map(p -> ((T) p.instance())).toList();
    }

    /**
     * Get a function that will take a {@link Settings} object and return a {@link PluginsService}.
     * This function passes in an empty list of classpath plugins.
     * @param environment The environment for the plugins service.
     * @return A function for creating a plugins service.
     */
    public static Function<Settings, PluginsService> getPluginsServiceCtor(Environment environment) {
        return settings -> new PluginsService(settings, environment.configFile(), environment.modulesFile(), environment.pluginsFile());
    }

    static final LayerAndLoader createPluginModuleLayer(
        PluginBundle bundle,
        ClassLoader parentLoader,
        List<ModuleLayer> parentLayers,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        assert bundle.plugin.getModuleName().isPresent();
        return createModuleLayer(
            bundle.plugin.getClassname(),
            bundle.plugin.getModuleName().get(),
            urlsToPaths(bundle.urls),
            parentLoader,
            parentLayers,
            qualifiedExports
        );
    }

    static final LayerAndLoader createSpiModuleLayer(
        Set<URL> urls,
        ClassLoader parentLoader,
        List<ModuleLayer> parentLayers,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        // assert bundle.plugin.getModuleName().isPresent();
        return createModuleLayer(
            null,  // no entry point
            spiModuleName(urls),
            urlsToPaths(urls),
            parentLoader,
            parentLayers,
            qualifiedExports
        );
    }

    private static final Module serverModule = PluginsService.class.getModule();

    static LayerAndLoader createModuleLayer(
        String className,
        String moduleName,
        Path[] paths,
        ClassLoader parentLoader,
        List<ModuleLayer> parentLayers,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        logger.debug(() -> "Loading bundle: creating module layer and loader for module " + moduleName);
        var finder = ModuleFinder.of(paths);

        var configuration = Configuration.resolveAndBind(
            ModuleFinder.of(),
            parentConfigurationOrBoot(parentLayers),
            finder,
            Set.of(moduleName)
        );
        var controller = privilegedDefineModulesWithOneLoader(configuration, parentLayersOrBoot(parentLayers), parentLoader);
        var pluginModule = controller.layer().findModule(moduleName).get();
        ensureEntryPointAccessible(controller, pluginModule, className);
        // export/open upstream modules to this plugin module
        exposeQualifiedExportsAndOpens(pluginModule, qualifiedExports);
        // configure qualified exports/opens to other modules/plugins
        addPluginExportsServices(qualifiedExports, controller);
        logger.debug(() -> "Loading bundle: created module layer and loader for module " + moduleName);
        return new LayerAndLoader(controller.layer(), privilegedFindLoader(controller.layer(), moduleName));
    }

    private static List<ModuleLayer> parentLayersOrBoot(List<ModuleLayer> parentLayers) {
        if (parentLayers == null || parentLayers.isEmpty()) {
            return List.of(ModuleLayer.boot());
        } else {
            return parentLayers;
        }
    }

    private static List<Configuration> parentConfigurationOrBoot(List<ModuleLayer> parentLayers) {
        if (parentLayers == null || parentLayers.isEmpty()) {
            return List.of(ModuleLayer.boot().configuration());
        } else {
            return parentLayers.stream().map(ModuleLayer::configuration).toList();
        }
    }

    /** Ensures that the plugins main class (its entry point), if any, is accessible to the server. */
    private static void ensureEntryPointAccessible(Controller controller, Module pluginModule, String className) {
        if (className != null) {
            controller.addOpens(pluginModule, toPackageName(className), serverModule);
        }
    }

    /**
     * Adds qualified exports and opens declared in other upstream modules to the target module.
     * This is required since qualified statements targeting yet-to-be-created modules, i.e. plugins,
     * are silently dropped when the boot layer is created.
     */
    private static void exposeQualifiedExportsAndOpens(Module target, Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        qualifiedExports.getOrDefault(target.getName(), List.of()).forEach(exportService -> exportService.addExportsAndOpens(target));
    }

    private static void addExportsService(
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        ModuleQualifiedExportsService exportsService,
        String moduleName
    ) {
        for (String targetName : exportsService.getTargets()) {
            logger.debug("Registered qualified export from module " + moduleName + " to " + targetName);
            qualifiedExports.computeIfAbsent(targetName, k -> new ArrayList<>()).add(exportsService);
        }
    }

    protected void addServerExportsService(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        final Module serverModule = PluginsService.class.getModule();
        var exportsService = new ModuleQualifiedExportsService(serverModule) {
            @Override
            protected void addExports(String pkg, Module target) {
                serverModule.addExports(pkg, target);
            }

            @Override
            protected void addOpens(String pkg, Module target) {
                serverModule.addOpens(pkg, target);
            }
        };
        addExportsService(qualifiedExports, exportsService, serverModule.getName());
    }

    private static void addPluginExportsServices(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports, Controller controller) {
        for (Module module : controller.layer().modules()) {
            var exportsService = new ModuleQualifiedExportsService(module) {
                @Override
                protected void addExports(String pkg, Module target) {
                    controller.addExports(module, pkg, target);
                }

                @Override
                protected void addOpens(String pkg, Module target) {
                    controller.addOpens(module, pkg, target);
                }
            };
            addExportsService(qualifiedExports, exportsService, module.getName());
        }
    }

    /** Determines the module name of the SPI module, given its URL. */
    static String spiModuleName(Set<URL> spiURLS) {
        ModuleFinder finder = ModuleFinder.of(urlsToPaths(spiURLS));
        var mrefs = finder.findAll();
        assert mrefs.size() == 1 : "Expected a single module, got:" + mrefs;
        return mrefs.stream().findFirst().get().descriptor().name();
    }

    /**
     * Tuple of module layer and loader.
     * Modular Plugins have a plugin specific loader and layer.
     * Non-Modular plugins have a plugin specific loader and the boot layer.
     */
    record LayerAndLoader(ModuleLayer layer, ClassLoader loader) {

        LayerAndLoader {
            Objects.requireNonNull(layer);
            Objects.requireNonNull(loader);
        }

        static LayerAndLoader ofLoader(ClassLoader loader) {
            return new LayerAndLoader(ModuleLayer.boot(), loader);
        }
    }

    @SuppressForbidden(reason = "I need to convert URL's to Paths")
    static final Path[] urlsToPaths(Set<URL> urls) {
        return urls.stream().map(PluginsService::uncheckedToURI).map(PathUtils::get).toArray(Path[]::new);
    }

    static final URI uncheckedToURI(URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new AssertionError(new IOException(e));
        }
    }

    static final String toPackageName(String className) {
        assert className.endsWith(".") == false;
        int index = className.lastIndexOf(".");
        if (index == -1) {
            throw new IllegalStateException("invalid class name:" + className);
        }
        return className.substring(0, index);
    }

    @SuppressWarnings("removal")
    private static void privilegedSetContextClassLoader(ClassLoader loader) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.currentThread().setContextClassLoader(loader);
            return null;
        });
    }

    @SuppressWarnings("removal")
    static Controller privilegedDefineModulesWithOneLoader(Configuration cf, List<ModuleLayer> parentLayers, ClassLoader parentLoader) {
        return AccessController.doPrivileged(
            (PrivilegedAction<Controller>) () -> ModuleLayer.defineModulesWithOneLoader(cf, parentLayers, parentLoader)
        );
    }

    @SuppressWarnings("removal")
    static ClassLoader privilegedFindLoader(ModuleLayer layer, String name) {
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> layer.findLoader(name));
    }
}
