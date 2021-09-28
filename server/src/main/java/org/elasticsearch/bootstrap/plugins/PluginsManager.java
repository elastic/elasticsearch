/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.InstallPluginProvider;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.RemovePluginProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.bootstrap.plugins.ProxyUtils.buildProxy;

public class PluginsManager {

    private final Logger logger;
    private final Environment env;

    public PluginsManager(Environment env) {
        this.env = env;
        this.logger = LogManager.getLogger(this.getClass());
    }

    public static boolean configExists(Environment env) {
        return Files.exists(env.configFile().resolve("elasticsearch-plugins.yml"));
    }

    public void synchronizePlugins() throws Exception {
        final Path configPath = this.env.configFile().resolve("elasticsearch-plugins.yml");
        final Path previousConfigPath = this.env.configFile().resolve(".elasticsearch-plugins.yml.cache");

        if (Files.exists(configPath) == false) {
            return;
        }

        if (Files.exists(env.pluginsFile()) == false) {
            throw new PluginSyncException("Plugins directory missing: " + env.pluginsFile());
        }

        // The builtin modules, which are plugins, but cannot be installed or removed.
        final Set<String> modules = getFileFromClasspath("modules", "/modules.txt");
        // The official plugins that can be installed simply by name.
        final Set<String> officialPlugins = getFileFromClasspath("official plugins", "/plugins.txt");

        // 1. Parse descriptor file
        final PluginsConfig pluginsConfig = PluginsConfig.parseConfig(configPath);
        pluginsConfig.validate(officialPlugins);

        // 2. Parse cached descriptor file, if it exists
        final Optional<PluginsConfig> cachedPluginsConfig = Files.exists(previousConfigPath)
            ? Optional.of(PluginsConfig.parseConfig(previousConfigPath))
            : Optional.empty();

        // 3. Get list of installed plugins
        final List<PluginInfo> existingPlugins;
        try {
            existingPlugins = getExistingPlugins(officialPlugins, this.env);
        } catch (IOException e) {
            throw new PluginSyncException("Failed to list existing plugins", e);
        }

        // 4. Calculate changes
        final List<PluginDescriptor> pluginsThatShouldExist = pluginsConfig.getPlugins();
        final List<PluginDescriptor> pluginsThatActuallyExist = existingPlugins.stream()
            .map(info -> new PluginDescriptor(info.getName()))
            .collect(Collectors.toList());
        final Set<String> existingPluginIds = pluginsThatActuallyExist.stream().map(PluginDescriptor::getId).collect(Collectors.toSet());

        final List<PluginDescriptor> pluginsToInstall = difference(pluginsThatShouldExist, pluginsThatActuallyExist);
        final List<PluginDescriptor> pluginsToRemove = difference(pluginsThatActuallyExist, pluginsThatShouldExist);

        // Candidates for upgrade are any plugin that already exists and isn't about to be removed.
        final List<PluginDescriptor> pluginsToMaybeUpgrade = difference(pluginsThatShouldExist, pluginsToRemove).stream()
            .filter(each -> existingPluginIds.contains(each.getId()))
            .collect(Collectors.toList());

        final List<PluginDescriptor> pluginsToUpgrade = getPluginsToUpgrade(
            // Remove plugins that we know are going to be uninstalled
            pluginsToMaybeUpgrade,
            cachedPluginsConfig,
            officialPlugins,
            existingPlugins
        );

        printRequiredChanges(pluginsToRemove, pluginsToInstall, pluginsToUpgrade);

        if (pluginsToRemove.isEmpty() && pluginsToInstall.isEmpty() && pluginsToUpgrade.isEmpty()) {
            return;
        }

        ClassLoader classLoader = buildClassLoader(env);

        @SuppressWarnings("unchecked")
        Class<InstallPluginProvider> installClass = (Class<InstallPluginProvider>) classLoader.loadClass(
            "org.elasticsearch.plugins.cli.InstallPluginAction"
        );
        @SuppressWarnings("unchecked")
        Class<RemovePluginProvider> removeClass = (Class<RemovePluginProvider>) classLoader.loadClass(
            "org.elasticsearch.plugins.cli.RemovePluginAction"
        );

        InstallPluginProvider pluginInstaller = installClass.getDeclaredConstructor(Environment.class, Boolean.class).newInstance(env, true);
        RemovePluginProvider pluginRemover = removeClass.getDeclaredConstructor(Environment.class).newInstance(env);

        // final PluginRemover pluginRemover = new PluginRemover(env, true);
        // final PluginInstaller pluginInstaller = new PluginInstaller(env, modules, officialPlugins);

        // 5. Remove any plugins that are not in the descriptor
        if (pluginsToRemove.isEmpty() == false) {
            pluginRemover.execute(pluginsToRemove);
        }

        // 6. Add any plugins that are in the descriptor but missing from disk
        if (pluginsToInstall.isEmpty() == false) {
            pluginInstaller.setProxy(buildProxy(pluginsConfig.getProxy()));
            pluginInstaller.execute(pluginsToInstall);
        }

        // 7. Upgrade plugins
        if (pluginsToUpgrade.isEmpty() == false) {
            pluginRemover.setPurge(false);
            pluginRemover.execute(pluginsToUpgrade);

            pluginInstaller.execute(pluginsToUpgrade);
        }

        // 8. Cached the applied config so that we can diff it on the next run.
        PluginsConfig.writeConfig(pluginsConfig, previousConfigPath);
    }

    private Set<String> getFileFromClasspath(String description, String path) throws PluginSyncException {
        final Set<String> lines;
        try (var stream = PluginsManager.class.getResourceAsStream(path)) {
            lines = Streams.readAllLines(stream).stream().map(String::trim).collect(Sets.toUnmodifiableSortedSet());
        } catch (final IOException e) {
            throw new PluginSyncException("Failed to load list of " + description, e);
        }
        return lines;
    }

    private List<PluginDescriptor> getPluginsToUpgrade(
        List<PluginDescriptor> pluginsToMaybeUpgrade,
        Optional<PluginsConfig> cachedPluginsConfig,
        Set<String> officialPlugins,
        List<PluginInfo> existingPlugins
    ) {
        final Map<String, String> cachedPluginIdToLocation = cachedPluginsConfig.map(
            config -> config.getPlugins().stream().collect(Collectors.toMap(PluginDescriptor::getId, PluginDescriptor::getLocation))
        ).orElse(Map.of());

        return pluginsToMaybeUpgrade.stream().filter(eachPlugin -> {
            final String eachPluginId = eachPlugin.getId();

            // If a plugin's location has changed, reinstall
            if (Objects.equals(eachPlugin.getLocation(), cachedPluginIdToLocation.get(eachPluginId)) == false) {
                logger.debug(
                    "Location for plugin [{}] has changed from [{}] to [{}], reinstalling",
                    eachPluginId,
                    cachedPluginIdToLocation.get(eachPluginId),
                    eachPlugin.getLocation()
                );
                return true;
            }

            if (officialPlugins.contains(eachPluginId)) {
                // Find the currently installed plugin and check whether the version is lower than
                // the current node's version.
                final PluginInfo info = existingPlugins.stream()
                    .filter(each -> each.getName().equals(eachPluginId))
                    .findFirst()
                    .orElseThrow(
                        () -> {
                            // It should be literally impossible for us not to find a matching existing plugin. We derive
                            // the list of existing plugin IDs from the list of installed plugins.
                            throw new RuntimeException("Couldn't find a PluginInfo for [" + eachPluginId + "], which should be impossible");
                        }
                    );

                if (info.getElasticsearchVersion().before(Version.CURRENT)) {
                    logger.debug(
                        "Official plugin [{}] is out-of-date ({} versus {}), upgrading",
                        eachPluginId,
                        info.getElasticsearchVersion(),
                        Version.CURRENT
                    );
                    return true;
                }
                return false;
            }

            // Else don't upgrade.
            return false;
        }).collect(Collectors.toList());
    }

    private List<PluginInfo> getExistingPlugins(Set<String> officialPlugins, Environment env) throws IOException {
        final List<PluginInfo> plugins = new ArrayList<>();

        try (DirectoryStream<Path> paths = Files.newDirectoryStream(env.pluginsFile())) {
            for (Path pluginPath : paths) {
                String filename = pluginPath.getFileName().toString();
                if (filename.startsWith(".")) {
                    continue;
                }

                PluginInfo info = PluginInfo.readFromProperties(env.pluginsFile().resolve(pluginPath));
                plugins.add(info);

                // Check for a version mismatch, unless it's an official plugin since we can upgrade them.
                if (officialPlugins.contains(info.getName()) && info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
                    this.logger.warn(
                        "WARNING: plugin [{}] was built for Elasticsearch version {} but version {} is required",
                        info.getName(),
                        info.getElasticsearchVersion(),
                        Version.CURRENT
                    );
                }
            }
        }

        plugins.sort(Comparator.comparing(PluginInfo::getName));
        return plugins;
    }

    /**
     * Returns a list of all elements in {@code left} that are not present in {@code right}.
     * <p>
     * Comparisons are based solely using {@link PluginDescriptor#getId()}.
     *
     * @param left the items that may be retained
     * @param right the items that may be removed
     * @return a list of the remaining elements
     */
    private static List<PluginDescriptor> difference(List<PluginDescriptor> left, List<PluginDescriptor> right) {
        return left.stream().filter(eachDescriptor -> {
            final String id = eachDescriptor.getId();
            return right.stream().anyMatch(p -> p.getId().equals(id)) == false;
        }).collect(Collectors.toList());
    }

    private void printRequiredChanges(
        List<PluginDescriptor> pluginsToRemove,
        List<PluginDescriptor> pluginsToInstall,
        List<PluginDescriptor> pluginsToUpgrade
    ) {
        final BiConsumer<String, List<PluginDescriptor>> printSummary = (action, plugins) -> {
            if (plugins.isEmpty() == false) {
                List<String> pluginIds = plugins.stream().map(PluginDescriptor::getId).collect(Collectors.toList());
                this.logger.info("Plugins to be {}d: {}", action, pluginIds);
            }
        };

        if (pluginsToInstall.isEmpty() && pluginsToRemove.isEmpty() && pluginsToUpgrade.isEmpty()) {
            this.logger.info("No plugins to install, remove or upgrade");
        } else {
            printSummary.accept("remove", pluginsToRemove);
            printSummary.accept("install", pluginsToInstall);
            printSummary.accept("upgrade", pluginsToUpgrade);
        }
    }

    private ClassLoader buildClassLoader(Environment env) throws PluginSyncException {
        try {
            final URL pluginCli = env.libFile().resolve("tools").resolve("plugin-cli").resolve("*").toUri().toURL();
            return URLClassLoader.newInstance(new URL[] { pluginCli }, PluginsManager.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new PluginSyncException("Failed to build URL for plugin-cli jars", e);
        }
    }
}
