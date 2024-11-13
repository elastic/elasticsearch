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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;

import java.io.IOException;
import java.lang.ModuleLayer.Controller;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;
import static org.elasticsearch.jdk.ModuleQualifiedExportsService.addExportsService;
import static org.elasticsearch.jdk.ModuleQualifiedExportsService.exposeQualifiedExportsAndOpens;

public class PluginsLoader {

    /**
     * Contains information about the {@link ClassLoader}s and {@link ModuleLayer} required for loading a plugin
     * @param pluginClassLoader The {@link ClassLoader} used to instantiate the main class for the plugin
     * @param spiClassLoader The exported {@link ClassLoader} visible to other Java modules
     * @param spiModuleLayer The exported {@link ModuleLayer} visible to other Java modules
     */
    public record LoadedPluginLayer(ClassLoader pluginClassLoader, ClassLoader spiClassLoader, ModuleLayer spiModuleLayer) {

        public LoadedPluginLayer {
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
    }

    private static final Logger logger = LogManager.getLogger(PluginsLoader.class);
    private static final Module serverModule = PluginsLoader.class.getModule();

    private final Settings settings;
    private final Path configPath;

    private final Map<String, LoadedPluginLayer> loadedPluginLayers;

    /**
     * Constructs a new PluginsLoader
     *
     * @param settings         The settings of the system
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem
     */
    @SuppressWarnings("this-escape")
    public PluginsLoader(Settings settings, Path configPath, Path modulesDirectory, Path pluginsDirectory) {
        this.settings = settings;
        this.configPath = configPath;

        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports = new HashMap<>(ModuleQualifiedExportsService.getBootServices());
        addServerExportsService(qualifiedExports);

        Set<PluginBundle> seenBundles = new LinkedHashSet<>();

        // load (elasticsearch) module layers
        if (modulesDirectory != null) {
            try {
                Set<PluginBundle> modules = PluginsUtils.getModuleBundles(modulesDirectory);
                seenBundles.addAll(modules);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // load plugin layers
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    PluginsUtils.checkForFailedPluginRemovals(pluginsDirectory);
                    Set<PluginBundle> plugins = PluginsUtils.getPluginBundles(pluginsDirectory);
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        this.loadedPluginLayers = loadBundles(seenBundles, qualifiedExports);
    }

    private Map<String, LoadedPluginLayer> loadBundles(
        Set<PluginBundle> bundles,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        Map<String, LoadedPluginLayer> loaded = new LinkedHashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        if (sortedBundles.isEmpty() == false) {
            Set<URL> systemLoaderURLs = JarHell.parseModulesAndClassPath();
            for (PluginBundle bundle : sortedBundles) {
                PluginsUtils.checkBundleJarHell(systemLoaderURLs, bundle, transitiveUrls);
                loadBundle(bundle, loaded, qualifiedExports);
            }
        }

        return loaded;
    }

    private void loadBundle(
        PluginBundle bundle,
        Map<String, LoadedPluginLayer> loaded,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
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
            getClass().getClassLoader(),
            extendedPlugins.stream().map(LoadedPluginLayer::spiClassLoader).toList()
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

        loaded.put(name, new LoadedPluginLayer(pluginClassLoader, spiLayerAndLoader.loader, spiLayerAndLoader.layer));
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

    static LayerAndLoader createPlugin(
        PluginBundle bundle,
        ClassLoader pluginParentLoader,
        List<LoadedPluginLayer> extendedPlugins,
        LayerAndLoader spiLayerAndLoader,
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports
    ) {
        final PluginDescriptor plugin = bundle.plugin;
        if (plugin.getModuleName().isPresent()) {
            logger.debug(() -> "Loading bundle: " + plugin.getName() + ", modular");
            var parentLayers = Stream.concat(
                Stream.ofNullable(spiLayerAndLoader != null ? spiLayerAndLoader.layer() : null),
                extendedPlugins.stream().map(LoadedPluginLayer::spiModuleLayer)
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
            qualifiedExports
        );
    }

    static LayerAndLoader createPluginModuleLayer(
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

    static final String toPackageName(String className) {
        assert className.endsWith(".") == false;
        int index = className.lastIndexOf('.');
        if (index == -1) {
            throw new IllegalStateException("invalid class name:" + className);
        }
        return className.substring(0, index);
    }

    @SuppressForbidden(reason = "I need to convert URL's to Paths")
    static final Path[] urlsToPaths(Set<URL> urls) {
        return urls.stream().map(PluginsService::uncheckedToURI).map(PathUtils::get).toArray(Path[]::new);
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

    private static List<ModuleLayer> parentLayersOrBoot(List<ModuleLayer> parentLayers) {
        if (parentLayers == null || parentLayers.isEmpty()) {
            return List.of(ModuleLayer.boot());
        } else {
            return parentLayers;
        }
    }

    protected void addServerExportsService(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
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
}
