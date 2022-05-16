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
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.Strings;
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
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginsService implements ReportingService<PluginsAndModules> {

    /**
     * A loaded plugin is one for which Elasticsearch has successfully constructed an instance of the plugin's class
     * @param info Metadata about the plugin, usually loaded from plugin properties
     * @param instance The constructed instance of the plugin's main class
     * @param loader   The classloader for the plugin
     */
    record LoadedPlugin(PluginInfo info, Plugin instance, ClassLoader loader) {}

    private static final Logger logger = LogManager.getLogger(PluginsService.class);

    private final Settings settings;
    private final Path configPath;

    /**
     * We keep around a list of plugins and modules
     */
    private final List<LoadedPlugin> plugins;
    private final PluginsAndModules info;

    public static final Setting<List<String>> MANDATORY_SETTING = Setting.listSetting(
        "plugin.mandatory",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public List<Setting<?>> getPluginSettings() {
        return plugins.stream().flatMap(p -> p.instance().getSettings().stream()).toList();
    }

    public List<String> getPluginSettingsFilter() {
        return plugins.stream().flatMap(p -> p.instance().getSettingsFilter().stream()).toList();
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

        List<LoadedPlugin> pluginsLoaded = new ArrayList<>();
        List<PluginInfo> pluginsList = new ArrayList<>();
        // we need to build a List of plugins for checking mandatory plugins
        final List<String> pluginsNames = new ArrayList<>();

        // first we load plugins that are on the classpath. this is for tests
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginInfo pluginInfo = new PluginInfo(
                pluginClass.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                pluginClass.getName(),
                null,
                Collections.emptyList(),
                false,
                PluginType.ISOLATED,
                "",
                false
            );
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new LoadedPlugin(pluginInfo, plugin, null));
            pluginsList.add(pluginInfo);
            pluginsNames.add(pluginInfo.getName());
        }

        Set<PluginBundle> seenBundles = new LinkedHashSet<>();
        List<PluginInfo> modulesList = new ArrayList<>();
        // load modules
        if (modulesDirectory != null) {
            try {
                Set<PluginBundle> modules = PluginsUtils.getModuleBundles(modulesDirectory);
                for (PluginBundle bundle : modules) {
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
                    PluginsUtils.checkForFailedPluginRemovals(pluginsDirectory);
                    Set<PluginBundle> plugins = PluginsUtils.getPluginBundles(pluginsDirectory);
                    for (final PluginBundle bundle : plugins) {
                        pluginsList.add(bundle.plugin);
                        pluginsNames.add(bundle.plugin.getName());
                    }
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        Collection<LoadedPlugin> loaded = loadBundles(seenBundles);
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

    private static void logPluginInfo(final List<PluginInfo> pluginInfos, final String type, final Logger logger) {
        assert pluginInfos != null;
        if (pluginInfos.isEmpty()) {
            logger.info("no " + type + "s loaded");
        } else {
            for (final String name : pluginInfos.stream().map(PluginInfo::getName).sorted().toList()) {
                logger.info("loaded " + type + " [" + name + "]");
            }
        }
    }

    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (LoadedPlugin plugin : plugins) {
            Settings settings = plugin.instance().additionalSettings();
            for (String setting : settings.keySet()) {
                String oldPlugin = foundSettings.put(setting, plugin.info().getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException(
                        "Cannot have additional setting ["
                            + setting
                            + "] "
                            + "in plugin ["
                            + plugin.info().getName()
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
        for (final LoadedPlugin plugin : plugins) {
            builders.addAll(plugin.instance().getExecutorBuilders(settings));
        }
        return builders;
    }

    public void onIndexModule(IndexModule indexModule) {
        for (LoadedPlugin plugin : plugins) {
            plugin.instance().onIndexModule(indexModule);
        }
    }

    /**
     * Get information about plugins and modules
     */
    @Override
    public PluginsAndModules info() {
        return info;
    }

    private Collection<LoadedPlugin> loadBundles(Set<PluginBundle> bundles) {
        Map<String, LoadedPlugin> loaded = new HashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        Set<URL> systemLoaderURLs = JarHell.parseModulesAndClassPath();
        for (PluginBundle bundle : sortedBundles) {
            if (bundle.plugin.getType() != PluginType.BOOTSTRAP) {
                PluginsUtils.checkBundleJarHell(systemLoaderURLs, bundle, transitiveUrls);
                loadBundle(bundle, loaded);
            }
        }

        loadExtensions(loaded.values());
        return loaded.values();
    }

    // package-private for test visibility
    static void loadExtensions(Collection<LoadedPlugin> plugins) {

        Map<String, List<Plugin>> extendingPluginsByName = plugins.stream()
            .flatMap(t -> t.info().getExtendedPlugins().stream().map(extendedPlugin -> Tuple.tuple(extendedPlugin, t.instance())))
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));
        for (LoadedPlugin pluginTuple : plugins) {
            if (pluginTuple.instance() instanceof ExtensiblePlugin) {
                loadExtensionsForPlugin(
                    (ExtensiblePlugin) pluginTuple.instance(),
                    extendingPluginsByName.getOrDefault(pluginTuple.info().getName(), List.of())
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

    private Plugin loadBundle(PluginBundle bundle, Map<String, LoadedPlugin> loaded) {
        String name = bundle.plugin.getName();

        PluginsUtils.verifyCompatibility(bundle.plugin);

        // collect loaders of extended plugins
        List<ClassLoader> extendedLoaders = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            LoadedPlugin extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            if (ExtensiblePlugin.class.isInstance(extendedPlugin.instance()) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            assert extendedPlugin.loader() != null : "All non-classpath plugins should be loaded with a classloader";
            extendedLoaders.add(extendedPlugin.loader());
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
            loaded.put(name, new LoadedPlugin(bundle.plugin, plugin, spiLoader));
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
        return plugins.stream().filter(x -> type.isAssignableFrom(x.instance().getClass())).map(p -> ((T) p.instance())).toList();
    }
}
