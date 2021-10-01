/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli.action;

import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.SyncPluginsProvider;

import java.io.IOException;
import java.net.Proxy;
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

public class SyncPluginsAction implements SyncPluginsProvider {
    private final Terminal terminal;
    private final Environment env;

    public SyncPluginsAction(Terminal terminal, Environment env) {
        this.terminal = terminal;
        this.env = env;
    }

    public void execute() throws Exception {
        final Path configPath = this.env.configFile().resolve("elasticsearch-plugins.yml");
        final Path previousConfigPath = this.env.configFile().resolve(".elasticsearch-plugins.yml.cache");

        if (Files.exists(configPath) == false) {
            return;
        }

        if (Files.exists(env.pluginsFile()) == false) {
            throw new PluginSyncException("Plugins directory missing: " + env.pluginsFile());
        }

        // Parse descriptor file
        final PluginsConfig pluginsConfig = PluginsConfig.parseConfig(configPath);
        pluginsConfig.validate(InstallPluginAction.OFFICIAL_PLUGINS);

        // Parse cached descriptor file, if it exists
        final Optional<PluginsConfig> cachedPluginsConfig = Files.exists(previousConfigPath)
            ? Optional.of(PluginsConfig.parseConfig(previousConfigPath))
            : Optional.empty();

        final PluginChanges changes = getPluginChanges(pluginsConfig, cachedPluginsConfig);

        if (changes.isEmpty()) {
            terminal.println("No plugins to install, remove or upgrade");
            return;
        }

        performSync(pluginsConfig, changes);

        // 8. Cached the applied config so that we can diff it on the next run.
        PluginsConfig.writeConfig(pluginsConfig, previousConfigPath);
    }

    private PluginChanges getPluginChanges(PluginsConfig pluginsConfig, Optional<PluginsConfig> cachedPluginsConfig)
        throws PluginSyncException {
        final List<PluginInfo> existingPlugins = getExistingPlugins(this.env);

        final List<PluginDescriptor> pluginsThatShouldExist = pluginsConfig.getPlugins();
        final List<PluginDescriptor> pluginsThatActuallyExist = existingPlugins.stream()
            .map(info -> new PluginDescriptor(info.getName()))
            .collect(Collectors.toList());
        final Set<String> existingPluginIds = pluginsThatActuallyExist.stream().map(PluginDescriptor::getId).collect(Collectors.toSet());

        final List<PluginDescriptor> pluginsToInstall = difference(pluginsThatShouldExist, pluginsThatActuallyExist);
        final List<PluginDescriptor> pluginsToRemove = difference(pluginsThatActuallyExist, pluginsThatShouldExist);

        // Candidates for upgrade are any plugin that already exist and isn't about to be removed.
        final List<PluginDescriptor> pluginsToMaybeUpgrade = difference(pluginsThatShouldExist, pluginsToRemove).stream()
            .filter(each -> existingPluginIds.contains(each.getId()))
            .collect(Collectors.toList());

        final List<PluginDescriptor> pluginsToUpgrade = getPluginsToUpgrade(pluginsToMaybeUpgrade, cachedPluginsConfig, existingPlugins);

        return new PluginChanges(pluginsToRemove, pluginsToInstall, pluginsToUpgrade);
    }

    private void performSync(PluginsConfig pluginsConfig, PluginChanges changes) throws Exception {
        logRequiredChanges(changes);

        final Proxy proxy = ProxyUtils.buildProxy(pluginsConfig.getProxy());

        final RemovePluginAction removePluginAction = new RemovePluginAction(terminal, env, true);
        final InstallPluginAction installPluginAction = new InstallPluginAction(terminal, env, true);

        // Remove any plugins that are not in the config file
        removePluginAction.execute(changes.remove);

        // Add any plugins that are in the config file but missing from disk
        installPluginAction.setProxy(proxy);
        installPluginAction.execute(changes.install);

        // Upgrade plugins
        removePluginAction.setPurge(false);
        removePluginAction.execute(changes.upgrade);
        installPluginAction.execute(changes.upgrade);
    }

    private List<PluginDescriptor> getPluginsToUpgrade(
        List<PluginDescriptor> pluginsToMaybeUpgrade,
        Optional<PluginsConfig> cachedPluginsConfig,
        List<PluginInfo> existingPlugins
    ) {
        final Map<String, String> cachedPluginIdToLocation = cachedPluginsConfig.map(
            config -> config.getPlugins().stream().collect(Collectors.toMap(PluginDescriptor::getId, PluginDescriptor::getLocation))
        ).orElse(Map.of());

        return pluginsToMaybeUpgrade.stream().filter(eachPlugin -> {
            final String eachPluginId = eachPlugin.getId();

            // If a plugin's location has changed, reinstall
            if (Objects.equals(eachPlugin.getLocation(), cachedPluginIdToLocation.get(eachPluginId)) == false) {
                this.terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    String.format(
                        "Location for plugin [%s] has changed from [%s] to [%s], reinstalling",
                        eachPluginId,
                        cachedPluginIdToLocation.get(eachPluginId),
                        eachPlugin.getLocation()
                    )
                );
                return true;
            }

            // Official plugins must be upgraded when an Elasticsearch node is upgraded.
            if (InstallPluginAction.OFFICIAL_PLUGINS.contains(eachPluginId)) {
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
                    this.terminal.println(
                        Terminal.Verbosity.VERBOSE,
                        String.format(
                            "Official plugin [%s] is out-of-date (%s versus %s), upgrading",
                            eachPluginId,
                            info.getElasticsearchVersion(),
                            Version.CURRENT
                        )
                    );
                    return true;
                }
                return false;
            }

            // Else don't upgrade.
            return false;
        }).collect(Collectors.toList());
    }

    private List<PluginInfo> getExistingPlugins(Environment env) throws PluginSyncException {
        final List<PluginInfo> plugins = new ArrayList<>();

        try {
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(env.pluginsFile())) {
                for (Path pluginPath : paths) {
                    String filename = pluginPath.getFileName().toString();
                    if (filename.startsWith(".")) {
                        continue;
                    }

                    PluginInfo info = PluginInfo.readFromProperties(env.pluginsFile().resolve(pluginPath));
                    plugins.add(info);

                    // Check for a version mismatch, unless it's an official plugin since we can upgrade them.
                    if (InstallPluginAction.OFFICIAL_PLUGINS.contains(info.getName())
                        && info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
                        this.terminal.errorPrintln(
                            String.format(
                                "WARNING: plugin [%s] was built for Elasticsearch version %s but version %s is required",
                                info.getName(),
                                info.getElasticsearchVersion(),
                                Version.CURRENT
                            )
                        );
                    }
                }
            }
        } catch (IOException e) {
            throw new PluginSyncException("Failed to list existing plugins", e);
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

    private void logRequiredChanges(PluginChanges changes) {
        final BiConsumer<String, List<PluginDescriptor>> printSummary = (action, plugins) -> {
            if (plugins.isEmpty() == false) {
                List<String> pluginIds = plugins.stream().map(PluginDescriptor::getId).collect(Collectors.toList());
                this.terminal.errorPrintln(String.format("Plugins to be %s: %s", action, pluginIds));
            }
        };

        printSummary.accept("removed", changes.remove);
        printSummary.accept("installed", changes.install);
        printSummary.accept("upgraded", changes.upgrade);
    }

    private static class PluginChanges {
        final List<PluginDescriptor> remove;
        final List<PluginDescriptor> install;
        final List<PluginDescriptor> upgrade;

        private PluginChanges(List<PluginDescriptor> remove, List<PluginDescriptor> install, List<PluginDescriptor> upgrade) {
            this.remove = Objects.requireNonNull(remove);
            this.install = Objects.requireNonNull(install);
            this.upgrade = Objects.requireNonNull(upgrade);
        }

        boolean isEmpty() {
            return remove.isEmpty() && install.isEmpty() && upgrade.isEmpty();
        }
    }
}
