/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli.action;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RemovePluginProblem;
import org.elasticsearch.plugins.RemovePluginProvider;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * An action for the plugin CLI to remove plugins from Elasticsearch.
 */
public class RemovePluginAction implements RemovePluginProvider {

    private final Terminal terminal;
    private final Environment env;
    private boolean purge;

    /**
     * Creates a new action.
     *
     * @param terminal   the terminal to use for input/output
     * @param env        the environment for the local node
     * @param purge      if true, plugin configuration files will be removed but otherwise preserved
     */
    public RemovePluginAction(Terminal terminal, Environment env, boolean purge) {
        this.terminal = terminal;
        this.env = env;
        this.purge = purge;
    }

    public boolean isPurge() {
        return purge;
    }

    public void setPurge(boolean purge) {
        this.purge = purge;
    }

    /**
     * Looks for problems that would prevent the specified plugins from being removed.
     * @param plugins the plugins to check
     * @return {@code null} if there are no problems, or a {@link Tuple} that indicates the type of problem,
     * and a descriptive message.
     * @throws IOException if a problem occurs loading the plugins that are currently installed.
     */
    public Tuple<RemovePluginProblem, String> checkRemovePlugins(List<PluginDescriptor> plugins) throws IOException {
        if (plugins == null || plugins.isEmpty()) {
            throw new IllegalArgumentException("At least one plugin ID is required");
        }

        final Set<PluginsService.Bundle> bundles = PluginsService.getPluginBundles(this.env.pluginsFile());

        for (PluginDescriptor plugin : plugins) {
            final List<String> usedBy = checkUsedByOtherPlugins(bundles, plugin);

            if (usedBy.isEmpty() == false) {
                final StringBuilder message = new StringBuilder().append("cannot remove plugin [")
                    .append(plugin.getId())
                    .append(" because it is extended by other plugins:\n");
                usedBy.forEach(each -> message.append("\t- ").append(each).append("\n"));
                return Tuple.tuple(RemovePluginProblem.STILL_USED, message.toString());
            }
        }

        return plugins.stream().map(this::canRemovePlugin).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private List<String> checkUsedByOtherPlugins(Set<PluginsService.Bundle> bundles, PluginDescriptor plugin) {
        final List<String> usedBy = new ArrayList<>();

        for (PluginsService.Bundle bundle : bundles) {
            for (String extendedPlugin : bundle.plugin.getExtendedPlugins()) {
                String pluginId = plugin.getId();
                if (extendedPlugin.equals(pluginId)) {
                    usedBy.add(pluginId);
                }
            }
        }

        return usedBy;
    }

    private Tuple<RemovePluginProblem, String> canRemovePlugin(PluginDescriptor plugin) {
        String pluginId = plugin.getId();
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
            return Tuple.tuple(
                RemovePluginProblem.NOT_FOUND,
                "plugin [" + pluginId + "] not found; run 'elasticsearch-plugin list' to get list of installed plugins"
            );
        }

        final Path pluginBinDir = env.binFile().resolve(pluginId);
        if (Files.exists(pluginBinDir)) {
            if (Files.isDirectory(pluginBinDir) == false) {
                return Tuple.tuple(RemovePluginProblem.BIN_FILE_NOT_DIRECTORY, "bin dir for [" + pluginId + "] is not a directory");
            }
        }

        return null;
    }

    /**
     * Remove the plugin specified by {@code pluginName}. You should call {@link #checkRemovePlugins(List)}
     * first, to ensure that the removal can proceed.
     *
     * @param plugins    the IDs of the plugins to remove
     * @throws IOException   if any I/O exception occurs while performing a file operation
     */
    public void removePlugins(List<PluginDescriptor> plugins) throws IOException {
        if (plugins == null || plugins.isEmpty()) {
            throw new IllegalArgumentException("At least one plugin ID is required");
        }

        for (PluginDescriptor plugin : plugins) {
            removePlugin(plugin);
        }
    }

    private void removePlugin(PluginDescriptor plugin) throws IOException {
        final String pluginId = plugin.getId();
        final Path pluginDir = env.pluginsFile().resolve(pluginId);
        final Path pluginConfigDir = env.configFile().resolve(pluginId);
        final Path removing = env.pluginsFile().resolve(".removing-" + pluginId);

        terminal.println("-> removing [" + pluginId + "]...");

        final List<Path> pluginPaths = new ArrayList<>();

        /*
         * Add the contents of the plugin directory before creating the marker file and adding it to the list of paths to be deleted so
         * that the marker file is the last file to be deleted.
         */
        if (Files.exists(pluginDir)) {
            try (Stream<Path> paths = Files.list(pluginDir)) {
                pluginPaths.addAll(paths.collect(Collectors.toList()));
            }
            terminal.println(VERBOSE, "removing [" + pluginDir + "]");
        }

        final Path pluginBinDir = env.binFile().resolve(pluginId);
        if (Files.exists(pluginBinDir)) {
            try (Stream<Path> paths = Files.list(pluginBinDir)) {
                pluginPaths.addAll(paths.collect(Collectors.toList()));
            }
            pluginPaths.add(pluginBinDir);
            terminal.println(VERBOSE, "removing [" + pluginBinDir + "]");
        }

        if (Files.exists(pluginConfigDir)) {
            if (this.purge) {
                try (Stream<Path> paths = Files.list(pluginConfigDir)) {
                    pluginPaths.addAll(paths.collect(Collectors.toList()));
                }
                pluginPaths.add(pluginConfigDir);
                terminal.println(VERBOSE, "removing [" + pluginConfigDir + "]");
            } else {
                /*
                 * By default we preserve the config files in case the user is upgrading the plugin, but we print a message so the user
                 * knows in case they want to remove manually.
                 */
                final String message = String.format(
                    Locale.ROOT,
                    "-> preserving plugin config files [%s] in case of upgrade; use --purge if not needed",
                    pluginConfigDir
                );
                terminal.println(message);
            }
        }

        /*
         * We are going to create a marker file in the plugin directory that indicates that this plugin is a state of removal. If the
         * removal fails, the existence of this marker file indicates that the plugin is in a garbage state. We check for existence of this
         * marker file during startup so that we do not startup with plugins in such a garbage state. Up to this point, we have not done
         * anything destructive, so we create the marker file as the last action before executing destructive operations. We place this
         * marker file in the root plugin directory (not the specific plugin directory) so that we do not have to create the specific plugin
         * directory if it does not exist (we are purging configuration files).
         */
        try {
            Files.createFile(removing);
        } catch (final FileAlreadyExistsException e) {
            /*
             * We need to suppress the marker file already existing as we could be in this state if a previous removal attempt failed and
             * the user is attempting to remove the plugin again.
             */
            terminal.println(VERBOSE, "marker file [" + removing + "] already exists");
        }

        // add the plugin directory
        pluginPaths.add(pluginDir);

        // finally, add the marker file
        pluginPaths.add(removing);

        IOUtils.rm(pluginPaths.toArray(new Path[0]));
    }
}
