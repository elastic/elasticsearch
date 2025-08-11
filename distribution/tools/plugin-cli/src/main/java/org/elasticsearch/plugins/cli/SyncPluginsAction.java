/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginsSynchronizer;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.net.Proxy;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * This action compares the contents of a configuration files, {@code elasticsearch-plugins.yml}, with the currently
 * installed plugins, and ensures that plugins are installed or removed accordingly.
 * <p>
 * This action cannot be called from the command line. It is used exclusively by Elasticsearch on startup, but only
 * if the config file exists and the distribution type allows it.
 */
public class SyncPluginsAction implements PluginsSynchronizer {
    public static final String ELASTICSEARCH_PLUGINS_YML = "elasticsearch-plugins.yml";
    public static final String ELASTICSEARCH_PLUGINS_YML_CACHE = ".elasticsearch-plugins.yml.cache";

    private final Terminal terminal;
    private final Environment env;

    public SyncPluginsAction(Terminal terminal, Environment env) {
        this.terminal = terminal;
        this.env = env;
    }

    /**
     * Ensures that the plugin config file does <b>not</b> exist.
     * @param env the environment to check
     * @throws UserException if a plugins config file is found.
     */
    public static void ensureNoConfigFile(Environment env) throws UserException {
        final Path pluginsConfig = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(pluginsConfig)) {
            throw new UserException(
                ExitCodes.USAGE,
                "Plugins config ["
                    + pluginsConfig
                    + "] exists, which is used by Elasticsearch on startup to ensure the correct plugins "
                    + "are installed. Instead of using this tool, you need to update this config file and restart Elasticsearch."
            );
        }
    }

    /**
     * Synchronises plugins from the config file to the plugins dir.
     *
     * @throws Exception if anything goes wrong
     */
    @Override
    public void execute() throws Exception {
        final Path configPath = this.env.configFile().resolve(ELASTICSEARCH_PLUGINS_YML);
        final Path previousConfigPath = this.env.pluginsFile().resolve(ELASTICSEARCH_PLUGINS_YML_CACHE);

        if (Files.exists(configPath) == false) {
            // The `PluginsManager` will have checked that this file exists before invoking the action.
            throw new PluginSyncException("Plugins config does not exist: " + configPath.toAbsolutePath());
        }

        if (Files.exists(env.pluginsFile()) == false) {
            throw new PluginSyncException("Plugins directory missing: " + env.pluginsFile());
        }

        // Parse descriptor file
        final PluginsConfig pluginsConfig = PluginsConfig.parseConfig(configPath, YamlXContent.yamlXContent);
        pluginsConfig.validate(InstallPluginAction.OFFICIAL_PLUGINS);

        // Parse cached descriptor file, if it exists
        final Optional<PluginsConfig> cachedPluginsConfig = Files.exists(previousConfigPath)
            ? Optional.of(PluginsConfig.parseConfig(previousConfigPath, CborXContent.cborXContent))
            : Optional.empty();

        final PluginChanges changes = getPluginChanges(pluginsConfig, cachedPluginsConfig);

        if (changes.isEmpty()) {
            terminal.println("No plugins to install, remove or upgrade");
            return;
        }

        performSync(pluginsConfig, changes);

        // 8. Cached the applied config so that we can diff it on the next run.
        PluginsConfig.writeConfig(CborXContent.cborXContent, pluginsConfig, previousConfigPath);
    }

    // @VisibleForTesting
    PluginChanges getPluginChanges(PluginsConfig pluginsConfig, Optional<PluginsConfig> cachedPluginsConfig) throws PluginSyncException {
        final List<PluginDescriptor> existingPlugins = getExistingPlugins();

        final List<InstallablePlugin> pluginsThatShouldExist = pluginsConfig.getPlugins();
        final List<InstallablePlugin> pluginsThatActuallyExist = existingPlugins.stream()
            .map(info -> new InstallablePlugin(info.getName()))
            .collect(Collectors.toList());
        final Set<String> existingPluginIds = pluginsThatActuallyExist.stream().map(InstallablePlugin::getId).collect(Collectors.toSet());

        final List<InstallablePlugin> pluginsToInstall = difference(pluginsThatShouldExist, pluginsThatActuallyExist);
        final List<InstallablePlugin> pluginsToRemove = difference(pluginsThatActuallyExist, pluginsThatShouldExist);

        // Candidates for upgrade are any plugin that already exist and isn't about to be removed.
        final List<InstallablePlugin> pluginsToMaybeUpgrade = difference(pluginsThatShouldExist, pluginsToRemove).stream()
            .filter(each -> existingPluginIds.contains(each.getId()))
            .collect(Collectors.toList());

        final List<InstallablePlugin> pluginsToUpgrade = getPluginsToUpgrade(pluginsToMaybeUpgrade, cachedPluginsConfig, existingPlugins);

        return new PluginChanges(pluginsToRemove, pluginsToInstall, pluginsToUpgrade);
    }

    private void performSync(PluginsConfig pluginsConfig, PluginChanges changes) throws Exception {
        final Proxy proxy = ProxyUtils.buildProxy(pluginsConfig.getProxy());

        final RemovePluginAction removePluginAction = new RemovePluginAction(terminal, env, true);
        final InstallPluginAction installPluginAction = new InstallPluginAction(terminal, env, true);
        installPluginAction.setProxy(proxy);

        performSync(installPluginAction, removePluginAction, changes);
    }

    // @VisibleForTesting
    void performSync(InstallPluginAction installAction, RemovePluginAction removeAction, PluginChanges changes) throws Exception {
        logRequiredChanges(changes);

        // Remove any plugins that are not in the config file
        if (changes.remove.isEmpty() == false) {
            removeAction.setPurge(true);
            removeAction.execute(changes.remove);
        }

        // Add any plugins that are in the config file but missing from disk
        if (changes.install.isEmpty() == false) {
            installAction.execute(changes.install);
        }

        // Upgrade plugins
        if (changes.upgrade.isEmpty() == false) {
            removeAction.setPurge(false);
            removeAction.execute(changes.upgrade);
            installAction.execute(changes.upgrade);
        }
    }

    private List<InstallablePlugin> getPluginsToUpgrade(
        List<InstallablePlugin> pluginsToMaybeUpgrade,
        Optional<PluginsConfig> cachedPluginsConfig,
        List<PluginDescriptor> existingPlugins
    ) {
        final Map<String, String> cachedPluginIdToLocation = cachedPluginsConfig.map(
            config -> config.getPlugins().stream().collect(Collectors.toMap(InstallablePlugin::getId, InstallablePlugin::getLocation))
        ).orElse(emptyMap());

        return pluginsToMaybeUpgrade.stream().filter(eachPlugin -> {
            final String eachPluginId = eachPlugin.getId();

            // If a plugin's location has changed, reinstall
            if (Objects.equals(eachPlugin.getLocation(), cachedPluginIdToLocation.get(eachPluginId)) == false) {
                this.terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    String.format(
                        Locale.ROOT,
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
                final PluginDescriptor info = existingPlugins.stream()
                    .filter(each -> each.getName().equals(eachPluginId))
                    .findFirst()
                    .orElseThrow(() -> {
                        // It should be literally impossible for us not to find a matching existing plugin. We derive
                        // the list of existing plugin IDs from the list of installed plugins.
                        throw new RuntimeException("Couldn't find a PluginInfo for [" + eachPluginId + "], which should be impossible");
                    });

                if (info.getElasticsearchVersion().before(Version.CURRENT)) {
                    this.terminal.println(
                        Terminal.Verbosity.VERBOSE,
                        String.format(
                            Locale.ROOT,
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

    private List<PluginDescriptor> getExistingPlugins() throws PluginSyncException {
        final List<PluginDescriptor> plugins = new ArrayList<>();

        try {
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(env.pluginsFile())) {
                for (Path pluginPath : paths) {
                    String filename = pluginPath.getFileName().toString();
                    if (filename.startsWith(".")) {
                        continue;
                    }

                    PluginDescriptor info = PluginDescriptor.readFromProperties(env.pluginsFile().resolve(pluginPath));
                    plugins.add(info);

                    // Check for a version mismatch, unless it's an official plugin since we can upgrade them.
                    if (InstallPluginAction.OFFICIAL_PLUGINS.contains(info.getName())
                        && info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
                        this.terminal.errorPrintln(
                            String.format(
                                Locale.ROOT,
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

        return plugins;
    }

    /**
     * Returns a list of all elements in {@code left} that are not present in {@code right}.
     * <p>
     * Comparisons are based solely using {@link InstallablePlugin#getId()}.
     *
     * @param left the items that may be retained
     * @param right the items that may be removed
     * @return a list of the remaining elements
     */
    private static List<InstallablePlugin> difference(List<InstallablePlugin> left, List<InstallablePlugin> right) {
        return left.stream().filter(eachDescriptor -> {
            final String id = eachDescriptor.getId();
            return right.stream().anyMatch(p -> p.getId().equals(id)) == false;
        }).collect(Collectors.toList());
    }

    private void logRequiredChanges(PluginChanges changes) {
        final BiConsumer<String, List<InstallablePlugin>> printSummary = (action, plugins) -> {
            if (plugins.isEmpty() == false) {
                List<String> pluginIds = plugins.stream().map(InstallablePlugin::getId).collect(Collectors.toList());
                this.terminal.errorPrintln(String.format(Locale.ROOT, "Plugins to be %s: %s", action, pluginIds));
            }
        };

        printSummary.accept("removed", changes.remove);
        printSummary.accept("installed", changes.install);
        printSummary.accept("upgraded", changes.upgrade);
    }

    // @VisibleForTesting
    static class PluginChanges {
        final List<InstallablePlugin> remove;
        final List<InstallablePlugin> install;
        final List<InstallablePlugin> upgrade;

        PluginChanges(List<InstallablePlugin> remove, List<InstallablePlugin> install, List<InstallablePlugin> upgrade) {
            this.remove = Objects.requireNonNull(remove);
            this.install = Objects.requireNonNull(install);
            this.upgrade = Objects.requireNonNull(upgrade);
        }

        boolean isEmpty() {
            return remove.isEmpty() && install.isEmpty() && upgrade.isEmpty();
        }
    }
}
