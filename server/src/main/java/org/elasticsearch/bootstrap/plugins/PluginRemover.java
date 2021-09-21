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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An action to remove plugins from Elasticsearch.
 */
class PluginRemover {
    private final Logger logger;
    private final Environment env;
    private boolean purge;

    PluginRemover(Environment env, boolean purge) {
        this.env = env;
        this.purge = purge;
        this.logger = LogManager.getLogger(PluginRemover.class);
    }

    public boolean isPurge() {
        return purge;
    }

    public void setPurge(boolean purge) {
        this.purge = purge;
    }

    /**
     * Remove the plugin specified by {@code pluginName}.
     *
     * @param existingPlugins plugins that are already installed. Used to check that the remove can proceed.
     * @param pluginsToRemove    the IDs of the plugins to remove
     * @throws PluginSyncException   if any I/O exception occurs while performing a file operation
     * @throws PluginSyncException if plugins is null or empty
     * @throws PluginSyncException if plugin directory does not exist
     * @throws PluginSyncException if the plugin bin directory is not a directory
     */
    void execute(List<PluginInfo> existingPlugins, List<PluginDescriptor> pluginsToRemove) throws PluginSyncException {
        if (pluginsToRemove == null || pluginsToRemove.isEmpty()) {
            throw new PluginSyncException("plugins should not be null or empty");
        }

        ensurePluginsNotUsedByOtherPlugins(existingPlugins, pluginsToRemove);

        for (PluginDescriptor plugin : pluginsToRemove) {
            checkCanRemove(plugin);
        }

        for (PluginDescriptor plugin : pluginsToRemove) {
            removePlugin(plugin);
        }
    }

    private void checkCanRemove(PluginDescriptor plugin) throws PluginSyncException {
        final String pluginId = plugin.getId();
        final Path pluginDir = env.pluginsFile().resolve(pluginId);
        final Path pluginConfigDir = env.configFile().resolve(pluginId);
        final Path removing = env.pluginsFile().resolve(".removing-" + pluginId);

        /*
         * If the plugin does not exist and the plugin config does not exist, fail to the user that the plugin is not found, unless there's
         * a marker file left from a previously failed attempt in which case we proceed to clean up the marker file. Or, if the plugin does
         * not exist, the plugin config does, and we are not purging, again fail to the user that the plugin is not found.
         */
        if ((Files.exists(pluginDir) == false && Files.exists(pluginConfigDir) == false && Files.exists(removing) == false)
            || (Files.exists(pluginDir) == false && Files.exists(pluginConfigDir) && this.purge == false)) {
            final String message = String.format(
                Locale.ROOT,
                "plugin [%s] not found; run 'elasticsearch-plugin list' to get list of installed plugins",
                pluginId
            );
            throw new PluginSyncException(message);
        }
    }

    private void removePlugin(PluginDescriptor plugin) throws PluginSyncException {
        final String pluginId = plugin.getId();
        final Path pluginDir = env.pluginsFile().resolve(pluginId);
        final Path pluginConfigDir = env.configFile().resolve(pluginId);
        final Path removing = env.pluginsFile().resolve(".removing-" + pluginId);

        logger.debug("Removing [" + pluginId + "]...");

        final List<Path> pluginPaths = new ArrayList<>();

        /*
         * Add the contents of the plugin directory before creating the marker file and adding it to
         * the list of paths to be deleted so that the marker file is the last file to be deleted.
         */
        if (Files.exists(pluginDir)) {
            try (Stream<Path> paths = Files.list(pluginDir)) {
                pluginPaths.addAll(paths.collect(Collectors.toList()));
            } catch (IOException e) {
                throw new PluginSyncException("Error while listing files for plugin " + pluginId + ": " + e.getMessage(), e);
            }
            logger.debug("Removing directory [" + pluginDir + "]");
        }

        if (Files.exists(pluginConfigDir) && this.purge) {
            try (Stream<Path> paths = Files.list(pluginConfigDir)) {
                pluginPaths.addAll(paths.collect(Collectors.toList()));
            } catch (IOException e) {
                throw new PluginSyncException("Error while listing config files for plugin " + pluginId + ": " + e.getMessage(), e);
            }
            pluginPaths.add(pluginConfigDir);
            logger.debug("Removing directory [" + pluginConfigDir + "]");
        }

        /*
         * We are going to create a marker file in the plugin directory that indicates that this
         * plugin is a state of removal. If the removal fails, the existence of this marker file
         * indicates that the plugin is in a garbage state. We check for existence of this marker
         * file during startup so that we do not startup with plugins in such a garbage state. Up to
         * this point, we have not done anything destructive, so we create the marker file as the
         * last action before executing destructive operations. We place this marker file in the
         * root plugin directory (not the specific plugin directory) so that we do not have to
         * create the specific plugin directory if it does not exist (we are purging configuration
         * files).
         */
        try {
            Files.createFile(removing);
        } catch (final FileAlreadyExistsException e) {
            // We need to suppress the marker file already existing as we could be in this state if
            // a previous removal attempt failed and the user is attempting to remove the plugin
            // again.
            logger.debug("Marker file [" + removing + "] already exists");
        } catch (IOException e) {
            throw new PluginSyncException("Error while creating removal marker file for plugin " + pluginId + ": " + e.getMessage(), e);
        }

        // add the plugin directory
        pluginPaths.add(pluginDir);

        // finally, add the marker file
        pluginPaths.add(removing);

        try {
            IOUtils.rm(pluginPaths.toArray(new Path[0]));
        } catch (IOException e) {
            throw new PluginSyncException("Error while removing files for " + pluginId + ": " + e.getMessage(), e);
        }
    }

    private void ensurePluginsNotUsedByOtherPlugins(List<PluginInfo> existingPlugins, List<PluginDescriptor> pluginsToRemove)
        throws PluginSyncException {

        // First make sure nothing extends this plugin
        final Map<String, List<String>> usedBy = new HashMap<>();

        for (PluginInfo existingPluginInfo : existingPlugins) {
            for (String extendedPlugin : existingPluginInfo.getExtendedPlugins()) {
                for (PluginDescriptor plugin : pluginsToRemove) {
                    String pluginId = plugin.getId();
                    if (extendedPlugin.equals(pluginId)) {
                        usedBy.computeIfAbsent(existingPluginInfo.getName(), (_key -> new ArrayList<>())).add(pluginId);
                    }
                }
            }
        }
        if (usedBy.isEmpty()) {
            return;
        }

        usedBy.forEach(
            (plugin, dependants) -> {
                logger.error("Cannot remove plugin [{}] as the following plugins depend on it: {}", plugin, dependants);
            }
        );

        throw new PluginSyncException("Cannot remove some plugins because there are have dependant plugins. See the log for details.");
    }

}
