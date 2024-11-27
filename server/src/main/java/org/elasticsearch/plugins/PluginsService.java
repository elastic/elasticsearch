/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.PluginsLoader.PluginLayer;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.plugins.spi.SPIClassIterator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PluginsService implements ReportingService<PluginsAndModules> {

    public StablePluginsRegistry getStablePluginRegistry() {
        return stablePluginsRegistry;
    }

    /**
     * A loaded plugin is one for which Elasticsearch has successfully constructed an instance of the plugin's class
     * @param descriptor Metadata about the plugin, usually loaded from plugin properties
     * @param instance The constructed instance of the plugin's main class
     */
    record LoadedPlugin(PluginDescriptor descriptor, Plugin instance, ClassLoader classLoader) {

        LoadedPlugin {
            Objects.requireNonNull(descriptor);
            Objects.requireNonNull(instance);
        }
    }

    private static final Logger logger = LogManager.getLogger(PluginsService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(PluginsService.class);

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
     * @param settings The settings for this node
     * @param configPath The configuration path for this node
     * @param pluginsLoader the information required to complete loading of plugins
     */
    public PluginsService(Settings settings, Path configPath, PluginsLoader pluginsLoader) {
        Map<String, LoadedPlugin> loadedPlugins = loadPluginBundles(settings, configPath, pluginsLoader);

        var modulesDescriptors = pluginsLoader.moduleDescriptors();
        var pluginDescriptors = pluginsLoader.pluginDescriptors();

        var inspector = PluginIntrospector.getInstance();
        this.info = new PluginsAndModules(getRuntimeInfos(inspector, pluginDescriptors, loadedPlugins), modulesDescriptors);
        this.plugins = List.copyOf(loadedPlugins.values());

        checkDeprecations(inspector, pluginDescriptors, loadedPlugins);

        checkMandatoryPlugins(
            pluginDescriptors.stream().map(PluginDescriptor::getName).collect(Collectors.toSet()),
            new HashSet<>(MANDATORY_SETTING.get(settings))
        );

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        Set<String> moduleNames = new HashSet<>(modulesDescriptors.stream().map(PluginDescriptor::getName).toList());
        for (String name : loadedPlugins.keySet()) {
            if (moduleNames.contains(name)) {
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

    private Map<String, LoadedPlugin> loadPluginBundles(Settings settings, Path configPath, PluginsLoader pluginsLoader) {
        Map<String, LoadedPlugin> loadedPlugins = new LinkedHashMap<>();
        pluginsLoader.pluginLayers().forEach(pl -> loadBundle(pl, loadedPlugins, settings, configPath));
        loadExtensions(loadedPlugins.values());
        return loadedPlugins;
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

    /**
     * Loads a single SPI extension.
     *
     * There should be no more than one extension found. If no service providers
     * are found, the supplied fallback is used.
     *
     * @param service the SPI class that should be loaded
     * @param fallback a supplier for an instance if no providers are found
     * @return an instance of the service
     * @param <T> the SPI service type
     */
    public <T> T loadSingletonServiceProvider(Class<T> service, Supplier<T> fallback) {
        var services = loadServiceProviders(service);
        if (services.size() > 1) {
            throw new IllegalStateException(String.format(Locale.ROOT, "More than one extension found for %s", service.getSimpleName()));
        } else if (services.isEmpty()) {
            return fallback.get();
        }
        return services.get(0);
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

    private void loadBundle(PluginLayer pluginLayer, Map<String, LoadedPlugin> loadedPlugins, Settings settings, Path configPath) {
        String name = pluginLayer.pluginBundle().plugin.getName();
        logger.debug(() -> "Loading plugin bundle: " + name);

        // validate the list of extended plugins
        List<LoadedPlugin> extendedPlugins = new ArrayList<>();
        for (String extendedPluginName : pluginLayer.pluginBundle().plugin.getExtendedPlugins()) {
            LoadedPlugin extendedPlugin = loadedPlugins.get(extendedPluginName);
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isInstance(extendedPlugin.instance()) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            extendedPlugins.add(extendedPlugin);
            logger.debug(
                () -> "Loading plugin bundle: "
                    + name
                    + ", ext plugins: "
                    + extendedPlugins.stream().map(lp -> lp.descriptor().getName()).toList()
            );
        }

        PluginBundle pluginBundle = pluginLayer.pluginBundle();
        ClassLoader pluginClassLoader = pluginLayer.pluginClassLoader();

        // reload SPI with any new services from the plugin
        reloadLuceneSPI(pluginLayer.pluginClassLoader());

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            // Set context class loader to plugin's class loader so that plugins
            // that have dependencies with their own SPI endpoints have a chance to load
            // and initialize them appropriately.
            privilegedSetContextClassLoader(pluginLayer.pluginClassLoader());

            Plugin plugin;
            if (pluginBundle.pluginDescriptor().isStable()) {
                stablePluginsRegistry.scanBundleForStablePlugins(pluginBundle, pluginClassLoader);
                /*
                Contrary to old plugins we don't need an instance of the plugin here.
                Stable plugin register components (like CharFilterFactory) in stable plugin registry, which is then used in AnalysisModule
                when registering char filter factories and other analysis components.
                We don't have to support for settings, additional components and other methods
                that are in org.elasticsearch.plugins.Plugin
                We need to pass a name though so that we can show that a plugin was loaded (via cluster state api)
                This might need to be revisited once support for settings is added
                 */
                plugin = new StablePluginPlaceHolder(pluginBundle.plugin.getName());
            } else {

                Class<? extends Plugin> pluginClass = loadPluginClass(pluginBundle.plugin.getClassname(), pluginClassLoader);
                if (pluginClassLoader != pluginClass.getClassLoader()) {
                    throw new IllegalStateException(
                        "Plugin ["
                            + name
                            + "] must reference a class loader local Plugin class ["
                            + pluginBundle.plugin.getClassname()
                            + "] (class loader ["
                            + pluginClass.getClassLoader()
                            + "])"
                    );
                }
                plugin = loadPlugin(pluginClass, settings, configPath);
            }
            loadedPlugins.put(name, new LoadedPlugin(pluginBundle.plugin, plugin, pluginLayer.pluginClassLoader()));
        } finally {
            privilegedSetContextClassLoader(cl);
        }
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
    public final <T> Stream<T> filterPlugins(Class<T> type) {
        return plugins().stream().filter(x -> type.isAssignableFrom(x.instance().getClass())).map(p -> ((T) p.instance()));
    }

    @SuppressWarnings("removal")
    private static void privilegedSetContextClassLoader(ClassLoader loader) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.currentThread().setContextClassLoader(loader);
            return null;
        });
    }
}
