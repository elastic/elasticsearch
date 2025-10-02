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
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;
import org.elasticsearch.nativeaccess.NativeAccessUtil;

import java.io.IOException;
import java.lang.ModuleLayer.Controller;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;
import static org.elasticsearch.jdk.ModuleQualifiedExportsService.addExportsService;
import static org.elasticsearch.jdk.ModuleQualifiedExportsService.exposeQualifiedExportsAndOpens;

/**
 * This class is used to load modules and module layers for each plugin during
 * node initialization prior to enablement of entitlements. This allows entitlements
 * to have all the plugin information they need prior to starting.
 */
public class PluginsLoader {
    /**
     * Contains information about the {@link ClassLoader} required to load a plugin
     */
    public interface PluginLayer {
        /**
         * @return Information about the bundle of jars used in this plugin
         */
        PluginBundle pluginBundle();

        /**
         * @return The {@link ClassLoader} used to instantiate the main class for the plugin
         */
        ClassLoader pluginClassLoader();

        /**
         * @return The {@link ModuleLayer} for the plugin modules
         */
        ModuleLayer pluginModuleLayer();
    }

    /**
     * Contains information about the {@link ClassLoader}s and {@link ModuleLayer} required for loading a plugin
     *
     * @param pluginBundle      Information about the bundle of jars used in this plugin
     * @param pluginClassLoader The {@link ClassLoader} used to instantiate the main class for the plugin
     * @param pluginModuleLayer The {@link ModuleLayer} containing the Java modules of the plugin
     * @param spiClassLoader    The exported {@link ClassLoader} visible to other Java modules
     * @param spiModuleLayer    The exported {@link ModuleLayer} visible to other Java modules
     */
    private record LoadedPluginLayer(
        PluginBundle pluginBundle,
        ClassLoader pluginClassLoader,
        ModuleLayer pluginModuleLayer,
        ClassLoader spiClassLoader,
        ModuleLayer spiModuleLayer
    ) implements PluginLayer {

        public LoadedPluginLayer {
            Objects.requireNonNull(pluginBundle);
            Objects.requireNonNull(pluginClassLoader);
            Objects.requireNonNull(spiClassLoader);
            Objects.requireNonNull(spiModuleLayer);
        }
    }

    /**
     * Tuple of module layer and loader.
     * Modular Plugins have a plugin specific loader and layer.
     * Non-Modular plugins have a plugin specific loader and the boot layer.
     */
    public record LayerAndLoader(ModuleLayer layer, ClassLoader loader) {

        public LayerAndLoader {
            Objects.requireNonNull(layer);
            Objects.requireNonNull(loader);
        }

        public static LayerAndLoader ofLoader(ClassLoader loader) {
            return new LayerAndLoader(ModuleLayer.boot(), loader);
        }

        public static LayerAndLoader ofUberModuleLoader(UberModuleClassLoader loader) {
            return new LayerAndLoader(loader.getLayer(), loader);
        }
    }

    private static final Logger logger = LogManager.getLogger(PluginsLoader.class);
    private static final Module serverModule = PluginsLoader.class.getModule();

    private final List<PluginDescriptor> moduleDescriptors;
    private final List<PluginDescriptor> pluginDescriptors;
    private final Map<String, LoadedPluginLayer> loadedPluginLayers;
    private final Set<PluginBundle> moduleBundles;
    private final Set<PluginBundle> pluginBundles;

    /**
     * Loads a set of PluginBundles from the modules directory
     *
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     */
    public static Set<PluginBundle> loadModulesBundles(Path modulesDirectory) {
        // load (elasticsearch) module layers
        final Set<PluginBundle> modules;
        if (modulesDirectory != null) {
            try {
                modules = PluginsUtils.getModuleBundles(modulesDirectory);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        } else {
            modules = Collections.emptySet();
        }
        return modules;
    }

    /**
     * Loads a set of PluginBundles from the plugins directory
     *
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     */
    public static Set<PluginBundle> loadPluginsBundles(Path pluginsDirectory) {
        final Set<PluginBundle> plugins;
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    PluginsUtils.checkForFailedPluginRemovals(pluginsDirectory);
                    plugins = PluginsUtils.getPluginBundles(pluginsDirectory);
                } else {
                    plugins = Collections.emptySet();
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        } else {
            plugins = Collections.emptySet();
        }
        return plugins;
    }

    /**
     * Constructs a new PluginsLoader
     *
     * @param modules The set of module bundles present on the filesystem
     * @param plugins The set of plugin bundles present on the filesystem
     * @param pluginsWithNativeAccess A map plugin name -> set of module names for which we want to enable native access
     */
    public static PluginsLoader createPluginsLoader(
        Set<PluginBundle> modules,
        Set<PluginBundle> plugins,
        Map<String, Set<String>> pluginsWithNativeAccess
    ) {
        return createPluginsLoader(modules, plugins, pluginsWithNativeAccess, true);
    }

    /**
     * Constructs a new PluginsLoader
     *
     * @param modules The set of module bundles present on the filesystem
     * @param plugins The set of plugin bundles present on the filesystem
     * @param pluginsWithNativeAccess A map plugin name -> set of module names for which we want to enable native access
     * @param withServerExports {@code true} to add server module exports
     */
    public static PluginsLoader createPluginsLoader(
        Set<PluginBundle> modules,
        Set<PluginBundle> plugins,
        Map<String, Set<String>> pluginsWithNativeAccess,
        boolean withServerExports
    ) {
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports;
        if (withServerExports) {
            qualifiedExports = new HashMap<>(ModuleQualifiedExportsService.getBootServices());
            addServerExportsService(qualifiedExports);
        } else {
            qualifiedExports = Collections.emptyMap();
        }

        Set<PluginBundle> seenBundles = new LinkedHashSet<>();
        seenBundles.addAll(modules);
        seenBundles.addAll(plugins);

        Map<String, LoadedPluginLayer> loadedPluginLayers = new LinkedHashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(seenBundles);
        if (sortedBundles.isEmpty() == false) {
            Set<URL> systemLoaderURLs = JarHell.parseModulesAndClassPath();
            for (PluginBundle bundle : sortedBundles) {
                PluginsUtils.checkBundleJarHell(systemLoaderURLs, bundle, transitiveUrls);
                var modulesWithNativeAccess = pluginsWithNativeAccess.getOrDefault(bundle.plugin.getName(), Set.of());
                loadPluginLayer(bundle, loadedPluginLayers, qualifiedExports, modulesWithNativeAccess);
            }
        }

        return new PluginsLoader(modules, plugins, loadedPluginLayers);
    }

    PluginsLoader(Set<PluginBundle> modules, Set<PluginBundle> plugins, Map<String, LoadedPluginLayer> loadedPluginLayers) {
        this.moduleBundles = modules;
        this.pluginBundles = plugins;
        this.moduleDescriptors = modules.stream().map(PluginBundle::pluginDescriptor).toList();
        this.pluginDescriptors = plugins.stream().map(PluginBundle::pluginDescriptor).toList();
        this.loadedPluginLayers = loadedPluginLayers;
    }

    public List<PluginDescriptor> moduleDescriptors() {
        return moduleDescriptors;
    }

    public List<PluginDescriptor> pluginDescriptors() {
        return pluginDescriptors;
    }

    public Stream<PluginLayer> pluginLayers() {
        return loadedPluginLayers.values().stream().map(Function.identity());
    }

    public Set<PluginBundle> moduleBundles() {
        return moduleBundles;
    }

    public Set<PluginBundle> pluginBundles() {
        return pluginBundles;
    }

    private static void loadPluginLayer(
        PluginBundle bundle,
        Map<String, LoadedPluginLayer> loaded,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        Set<String> modulesWithNativeAccess
    ) {
        String name = bundle.plugin.getName();
        logger.debug(() -> "Loading bundle: " + name);

        PluginsUtils.verifyCompatibility(bundle.plugin);

        // collect the list of extended plugins
        List<LoadedPluginLayer> extendedPlugins = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            LoadedPluginLayer extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            assert extendedPlugin.spiClassLoader() != null : "All non-classpath plugins should be loaded with a classloader";
            extendedPlugins.add(extendedPlugin);
        }

        final ClassLoader parentLoader = ExtendedPluginsClassLoader.create(
            PluginsLoader.class.getClassLoader(),
            extendedPlugins.stream().map(LoadedPluginLayer::spiClassLoader).toList()
        );
        LayerAndLoader spiLayerAndLoader = null;
        if (bundle.hasSPI()) {
            spiLayerAndLoader = createSPI(bundle, parentLoader, extendedPlugins, qualifiedExports);
        }

        final ClassLoader pluginParentLoader = spiLayerAndLoader == null ? parentLoader : spiLayerAndLoader.loader();
        final LayerAndLoader pluginLayerAndLoader = createPluginLayerAndLoader(
            bundle,
            pluginParentLoader,
            extendedPlugins,
            spiLayerAndLoader,
            qualifiedExports,
            modulesWithNativeAccess
        );
        final ClassLoader pluginClassLoader = pluginLayerAndLoader.loader();

        if (spiLayerAndLoader == null) {
            // use full implementation for plugins extending this one
            spiLayerAndLoader = pluginLayerAndLoader;
        }

        loaded.put(
            name,
            new LoadedPluginLayer(
                bundle,
                pluginClassLoader,
                pluginLayerAndLoader.layer(),
                spiLayerAndLoader.loader,
                spiLayerAndLoader.layer
            )
        );
    }

    static LayerAndLoader createSPI(
        PluginBundle bundle,
        ClassLoader parentLoader,
        List<LoadedPluginLayer> extendedPlugins,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        final PluginDescriptor plugin = bundle.plugin;
        if (plugin.getModuleName().isPresent()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", creating spi, modular");
            return createSpiModuleLayer(
                bundle.spiUrls,
                parentLoader,
                extendedPlugins.stream().map(LoadedPluginLayer::spiModuleLayer).toList(),
                qualifiedExports
            );
        } else {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", creating spi, non-modular");
            return LayerAndLoader.ofLoader(URLClassLoader.newInstance(bundle.spiUrls.toArray(new URL[0]), parentLoader));
        }
    }

    private static LayerAndLoader createPluginLayerAndLoader(
        PluginBundle bundle,
        ClassLoader pluginParentLoader,
        List<LoadedPluginLayer> extendedPlugins,
        LayerAndLoader spiLayerAndLoader,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        Set<String> modulesWithNativeAccess
    ) {
        final PluginDescriptor plugin = bundle.plugin;
        if (plugin.getModuleName().isPresent()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", modular");
            var parentLayers = Stream.concat(
                Stream.ofNullable(spiLayerAndLoader != null ? spiLayerAndLoader.layer() : null),
                extendedPlugins.stream().map(LoadedPluginLayer::spiModuleLayer)
            ).toList();
            return createPluginModuleLayer(bundle, pluginParentLoader, parentLayers, qualifiedExports, modulesWithNativeAccess);
        } else if (plugin.isStable()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", non-modular as synthetic module");
            return LayerAndLoader.ofUberModuleLoader(
                UberModuleClassLoader.getInstance(
                    pluginParentLoader,
                    ModuleLayer.boot(),
                    "synthetic." + toModuleName(plugin.getName()),
                    bundle.allUrls,
                    Set.of("org.elasticsearch.server"), // TODO: instead of denying server, allow only jvm + stable API modules
                    modulesWithNativeAccess
                )
            );
        } else {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", non-modular");
            return LayerAndLoader.ofLoader(URLClassLoader.newInstance(bundle.urls.toArray(URL[]::new), pluginParentLoader));
        }
    }

    static LayerAndLoader createSpiModuleLayer(
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
            qualifiedExports,
            Set.of()
        );
    }

    static LayerAndLoader createPluginModuleLayer(
        PluginBundle bundle,
        ClassLoader parentLoader,
        List<ModuleLayer> parentLayers,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        Set<String> modulesWithNativeAccess
    ) {
        assert bundle.plugin.getModuleName().isPresent();
        return createModuleLayer(
            bundle.plugin.getClassname(),
            bundle.plugin.getModuleName().get(),
            urlsToPaths(bundle.urls),
            parentLoader,
            parentLayers,
            qualifiedExports,
            modulesWithNativeAccess
        );
    }

    static LayerAndLoader createModuleLayer(
        String className,
        String moduleName,
        Path[] paths,
        ClassLoader parentLoader,
        List<ModuleLayer> parentLayers,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        Set<String> modulesWithNativeAccess
    ) {
        logger.debug(() -> "Loading bundle: creating module layer and loader for module " + moduleName);
        var finder = ModuleFinder.of(paths);

        var configuration = Configuration.resolveAndBind(
            ModuleFinder.of(),
            parentConfigurationOrBoot(parentLayers),
            finder,
            Set.of(moduleName)
        );
        var controller = ModuleLayer.defineModulesWithOneLoader(configuration, parentLayersOrBoot(parentLayers), parentLoader);
        var pluginModule = controller.layer().findModule(moduleName).get();
        ensureEntryPointAccessible(controller, pluginModule, className);
        // export/open upstream modules to this plugin module
        exposeQualifiedExportsAndOpens(pluginModule, qualifiedExports);
        // configure qualified exports/opens to other modules/plugins
        addPluginExportsServices(qualifiedExports, controller);
        enableNativeAccess(moduleName, modulesWithNativeAccess, controller);
        logger.debug(() -> "Loading bundle: created module layer and loader for module " + moduleName);
        return new LayerAndLoader(controller.layer(), controller.layer().findLoader(moduleName));
    }

    /** Determines the module name of the SPI module, given its URL. */
    static String spiModuleName(Set<URL> spiURLS) {
        ModuleFinder finder = ModuleFinder.of(urlsToPaths(spiURLS));
        var mrefs = finder.findAll();
        assert mrefs.size() == 1 : "Expected a single module, got:" + mrefs;
        return mrefs.stream().findFirst().get().descriptor().name();
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

    static String toPackageName(String className) {
        assert className.endsWith(".") == false;
        int index = className.lastIndexOf('.');
        if (index == -1) {
            throw new IllegalStateException("invalid class name:" + className);
        }
        return className.substring(0, index);
    }

    @SuppressForbidden(reason = "I need to convert URL's to Paths")
    static Path[] urlsToPaths(Set<URL> urls) {
        return urls.stream().map(PluginsLoader::uncheckedToURI).map(PathUtils::get).toArray(Path[]::new);
    }

    static URI uncheckedToURI(URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new AssertionError(new IOException(e));
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

    private static List<ModuleLayer> parentLayersOrBoot(List<ModuleLayer> parentLayers) {
        if (parentLayers == null || parentLayers.isEmpty()) {
            return List.of(ModuleLayer.boot());
        } else {
            return parentLayers;
        }
    }

    private static void addServerExportsService(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
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

    private static void enableNativeAccess(String mainModuleName, Set<String> modulesWithNativeAccess, Controller controller) {
        for (var moduleName : modulesWithNativeAccess) {
            var module = controller.layer().findModule(moduleName);
            module.ifPresentOrElse(m -> NativeAccessUtil.enableNativeAccess(controller, m), () -> {
                assert false
                    : Strings.format(
                        "Native access not enabled for module [%s]: not a valid module name in layer [%s]",
                        moduleName,
                        mainModuleName
                    );
            });
        }
    }
}
