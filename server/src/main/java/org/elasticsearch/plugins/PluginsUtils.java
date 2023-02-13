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
import java.util.HashMap;
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
    public static void verifyCompatibility(PluginDescriptor info) {
        if (info.isStable()) {
            if (info.getElasticsearchVersion().major != Version.CURRENT.major) {
                throw new IllegalArgumentException(
                    "Stable Plugin ["
                        + info.getName()
                        + "] was built for Elasticsearch major version "
                        + info.getElasticsearchVersion().major
                        + " but version "
                        + Version.CURRENT
                        + " is running"
                );
            }
            if (info.getElasticsearchVersion().after(Version.CURRENT)) {
                throw new IllegalArgumentException(
                    "Stable Plugin ["
                        + info.getName()
                        + "] was built for Elasticsearch version "
                        + info.getElasticsearchVersion()
                        + " but earlier version "
                        + Version.CURRENT
                        + " is running"
                );
            }
        } else if (info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
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

    /**
     * Check for the existence of a marker file that indicates any plugins are in a garbage state from a failed attempt to remove the
     * plugin.
     * @param pluginsDirectory Path to plugins directory
     * @throws IOException if there is an error reading from the filesystem
     */
    public static void checkForFailedPluginRemovals(final Path pluginsDirectory) throws IOException {
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
    static Set<PluginBundle> getModuleBundles(Path modulesDirectory) throws IOException {
        return findBundles(modulesDirectory, "module");
    }

    /** Get bundles for plugins installed in the given plugins directory. */
    static Set<PluginBundle> getPluginBundles(final Path pluginsDirectory) throws IOException {
        return findBundles(pluginsDirectory, "plugin");
    }

    /**
     * A convenience method for analyzing plugin dependencies
     * @param pluginsDirectory Directory of plugins to scan
     * @return a map of plugin names to a list of names of any plugins that they extend
     * @throws IOException if there is an error reading the plugins
     */
    public static Map<String, List<String>> getDependencyMapView(final Path pluginsDirectory) throws IOException {
        return getPluginBundles(pluginsDirectory).stream()
            .collect(Collectors.toMap(b -> b.plugin.getName(), b -> b.plugin.getExtendedPlugins()));
    }

    // searches subdirectories under the given directory for plugin directories
    private static Set<PluginBundle> findBundles(final Path directory, String type) throws IOException {
        final Set<PluginBundle> bundles = new HashSet<>();
        for (final Path plugin : findPluginDirs(directory)) {
            final PluginBundle bundle = readPluginBundle(plugin, type);
            // PluginInfo hashes on plugin name, so this will catch name clashes
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
        final PluginDescriptor info;
        try {
            info = PluginDescriptor.readFromProperties(plugin);
        } catch (final IOException e) {
            throw new IllegalStateException(
                "Could not load plugin descriptor for " + type + " directory [" + plugin.getFileName() + "]",
                e
            );
        }
        return new PluginBundle(info, plugin);
    }

    /**
     * Given a plugin that we would like to install, perform a series of "jar hell
     * checks to make sure that we don't have any classname conflicts. Some of these
     * checks are unique to the "pre-installation" scenario, but we also call the
     * {@link #checkBundleJarHell(Set, PluginBundle, Map)}.
     * @param candidateInfo Candidate for installation
     * @param candidateDir Directory containing the candidate plugin files
     * @param pluginsDir Directory containing already-installed plugins
     * @param modulesDir Directory containing Elasticsearch modules
     * @param classpath Set of URLs to use for a classpath
     * @throws IOException on failed plugin reads
     */
    public static void preInstallJarHellCheck(
        PluginDescriptor candidateInfo,
        Path candidateDir,
        Path pluginsDir,
        Path modulesDir,
        Set<URL> classpath
    ) throws IOException {
        // create list of current jars in classpath

        // read existing bundles. this does some checks on the installation too.
        Set<PluginBundle> bundles = new HashSet<>(getPluginBundles(pluginsDir));
        bundles.addAll(getModuleBundles(modulesDir));
        bundles.add(new PluginBundle(candidateInfo, candidateDir));
        List<PluginBundle> sortedBundles = sortBundles(bundles);

        // check jarhell of all plugins so we know this plugin and anything depending on it are ok together
        // TODO: optimize to skip any bundles not connected to the candidate plugin?
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        for (PluginBundle bundle : sortedBundles) {
            checkBundleJarHell(classpath, bundle, transitiveUrls);
        }

        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case users mess with jar files manually
    static void checkBundleJarHell(Set<URL> systemLoaderURLs, PluginBundle bundle, Map<String, Set<URL>> transitiveUrls) {
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
    static List<PluginBundle> sortBundles(Set<PluginBundle> bundles) {
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
