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
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.jdk.JarHell;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility methods for loading plugin information from disk and for sorting
 * lists of plugins
 */
public class PluginsUtils {

    private static final Logger logger = LogManager.getLogger(PluginsUtils.class);

    private PluginsUtils() {
        throw new AssertionError("This utility class should never be instantiated");
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
    public static void verifyCompatibility(PluginInfo info) {
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
    public static Set<PluginBundle> getModuleBundles(Path modulesDirectory) throws IOException {
        return findBundles(modulesDirectory, "module");
    }

    /** Get bundles for plugins installed in the given plugins directory. */
    public static Set<PluginBundle> getPluginBundles(final Path pluginsDirectory) throws IOException {
        return findBundles(pluginsDirectory, "plugin");
    }

    // searches subdirectories under the given directory for plugin directories
    private static Set<PluginBundle> findBundles(final Path directory, String type) throws IOException {
        final Set<PluginBundle> bundles = new HashSet<>();
        for (final Path plugin : findPluginDirs(directory)) {
            final PluginBundle bundle = readPluginBundle(plugin, type);
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
    private static PluginBundle readPluginBundle(final Path plugin, String type) throws IOException {
        final PluginInfo info;
        try {
            info = PluginInfo.readFromProperties(plugin);
        } catch (final IOException e) {
            throw new IllegalStateException(
                "Could not load plugin descriptor for " + type + " directory [" + plugin.getFileName() + "]",
                e
            );
        }
        return new PluginBundle(info, plugin);
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case users mess with jar files manually
    public static void checkBundleJarHell(Set<URL> systemLoaderURLs, PluginBundle bundle, Map<String, Set<URL>> transitiveUrls) {
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
            Set<URL> intersection = new HashSet<>(systemLoaderURLs);
            intersection.retainAll(bundle.allUrls);
            if (intersection.isEmpty() == false) {
                throw new IllegalStateException("jar hell! duplicate codebases between plugin and core: " + intersection);
            }
            // check we don't have conflicting classes
            Set<URL> union = new HashSet<>(systemLoaderURLs);
            union.addAll(bundle.allUrls);
            JarHell.checkJarHell(union, logger::debug);
        } catch (final IllegalStateException ise) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " due to jar hell", ise);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " while checking for jar hell", e);
        }
    }

    /**
     * Return the given bundles, sorted in dependency loading order.
     *
     * This sort is stable, so that if two plugins do not have any interdependency,
     * their relative order from iteration of the provided set will not change.
     *
     * @throws IllegalStateException if a dependency cycle is found
     */
    public static List<PluginBundle> sortBundles(Set<PluginBundle> bundles) {
        Map<String, PluginBundle> namedBundles = bundles.stream().collect(Collectors.toMap(b -> b.plugin.getName(), Function.identity()));
        LinkedHashSet<PluginBundle> sortedBundles = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();
        for (PluginBundle bundle : bundles) {
            addSortedBundle(bundle, namedBundles, sortedBundles, dependencyStack);
        }
        return new ArrayList<>(sortedBundles);
    }

    // add the given bundle to the sorted bundles, first adding dependencies
    private static void addSortedBundle(
        PluginBundle bundle,
        Map<String, PluginBundle> bundles,
        LinkedHashSet<PluginBundle> sortedBundles,
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
            PluginBundle depBundle = bundles.get(dependency);
            if (depBundle == null) {
                throw new IllegalArgumentException("Missing plugin [" + dependency + "], dependency of [" + name + "]");
            }
            addSortedBundle(depBundle, bundles, sortedBundles, dependencyStack);
            assert sortedBundles.contains(depBundle);
        }
        dependencyStack.remove(name);

        sortedBundles.add(bundle);
    }

}
