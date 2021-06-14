/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin CLI to remove plugins from Elasticsearch.
 */
class RemovePluginCommand extends EnvironmentAwareCommand {

    // exit codes for remove
    /** A plugin cannot be removed because it is extended by another plugin. */
    static final int PLUGIN_STILL_USED = 11;

    private final OptionSpec<Void> purgeOption;
    private final OptionSpec<String> arguments;

    RemovePluginCommand() {
        super("removes plugins from Elasticsearch");
        this.purgeOption = parser.acceptsAll(Arrays.asList("p", "purge"), "Purge plugin configuration files");
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        final List<String> pluginIds = arguments.values(options);
        final boolean purge = options.has(purgeOption);
        execute(terminal, env, pluginIds, purge);
    }

    /**
     * Remove the plugin specified by {@code pluginName}.
     *
     * @param terminal   the terminal to use for input/output
     * @param env        the environment for the local node
     * @param pluginIds  the IDs of the plugins to remove
     * @param purge      if true, plugin configuration files will be removed but otherwise preserved
     * @throws IOException   if any I/O exception occurs while performing a file operation
     * @throws UserException if pluginIds is null or empty
     * @throws UserException if plugin directory does not exist
     * @throws UserException if the plugin bin directory is not a directory
     */
    void execute(Terminal terminal, Environment env, List<String> pluginIds, boolean purge) throws IOException, UserException {
        if (pluginIds == null || pluginIds.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "At least one plugin ID is required");
        }

        ensurePluginsNotUsedByOtherPlugins(env, pluginIds);

        for (String pluginId : pluginIds) {
            checkCanRemove(env, pluginId, purge);
        }

        for (String pluginId : pluginIds) {
            removePlugin(env, terminal, pluginId, purge);
        }
    }

    private void ensurePluginsNotUsedByOtherPlugins(Environment env, List<String> pluginIds) throws IOException, UserException {
        // First make sure nothing extends this plugin
        final Map<String, List<String>> usedBy = new HashMap<>();
        Set<PluginsService.Bundle> bundles = PluginsService.getPluginBundles(env.pluginsFile());
        for (PluginsService.Bundle bundle : bundles) {
            for (String extendedPlugin : bundle.plugin.getExtendedPlugins()) {
                for (String pluginId : pluginIds) {
                    if (extendedPlugin.equals(pluginId)) {
                        usedBy.computeIfAbsent(bundle.plugin.getName(), (_key -> new ArrayList<>())).add(pluginId);
                    }
                }
            }
        }
        if (usedBy.isEmpty()) {
            return;
        }

        final StringJoiner message = new StringJoiner("\n");
        message.add("Cannot remove plugins because the following are extended by other plugins:");
        usedBy.forEach((key, value) -> {
            String s = "\t" + key + " used by " + value;
            message.add(s);
        });

        throw new UserException(PLUGIN_STILL_USED, message.toString());
    }

    private void checkCanRemove(Environment env, String pluginId, boolean purge) throws UserException {
        final Path pluginDir = env.pluginsFile().resolve(pluginId);
        final Path pluginConfigDir = env.configFile().resolve(pluginId);
        final Path removing = env.pluginsFile().resolve(".removing-" + pluginId);

        /*
         * If the plugin does not exist and the plugin config does not exist, fail to the user that the plugin is not found, unless there's
         * a marker file left from a previously failed attempt in which case we proceed to clean up the marker file. Or, if the plugin does
         * not exist, the plugin config does, and we are not purging, again fail to the user that the plugin is not found.
         */
        if ((Files.exists(pluginDir) == false && Files.exists(pluginConfigDir) == false && Files.exists(removing) == false)
            || (Files.exists(pluginDir) == false && Files.exists(pluginConfigDir) && purge == false)) {
            final String message = String.format(
                Locale.ROOT,
                "plugin [%s] not found; run 'elasticsearch-plugin list' to get list of installed plugins",
                pluginId
            );
            throw new UserException(ExitCodes.CONFIG, message);
        }

        final Path pluginBinDir = env.binFile().resolve(pluginId);
        if (Files.exists(pluginBinDir)) {
            if (Files.isDirectory(pluginBinDir) == false) {
                throw new UserException(ExitCodes.IO_ERROR, "bin dir for " + pluginId + " is not a directory");
            }
        }
    }

    private void removePlugin(Environment env, Terminal terminal, String pluginId, boolean purge) throws IOException {
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
            if (purge) {
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
