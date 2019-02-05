/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin CLI to remove a plugin from Elasticsearch.
 */
class RemovePluginCommand extends EnvironmentAwareCommand {

    // exit codes for remove
    /** A plugin cannot be removed because it is extended by another plugin. */
    static final int PLUGIN_STILL_USED = 11;

    private final OptionSpec<Void> purgeOption;
    private final OptionSpec<String> arguments;

    RemovePluginCommand() {
        super("removes a plugin from Elasticsearch");
        this.purgeOption = parser.acceptsAll(Arrays.asList("p", "purge"), "Purge plugin configuration files");
        this.arguments = parser.nonOptions("plugin name");
    }

    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        final String pluginName = arguments.value(options);
        final boolean purge = options.has(purgeOption);
        execute(terminal, env, pluginName, purge);
    }

    /**
     * Remove the plugin specified by {@code pluginName}.
     *
     * @param terminal   the terminal to use for input/output
     * @param env        the environment for the local node
     * @param pluginName the name of the plugin to remove
     * @param purge      if true, plugin configuration files will be removed but otherwise preserved
     * @throws IOException   if any I/O exception occurs while performing a file operation
     * @throws UserException if plugin name is null
     * @throws UserException if plugin directory does not exist
     * @throws UserException if the plugin bin directory is not a directory
     */
    void execute(Terminal terminal, Environment env, String pluginName, boolean purge) throws IOException, UserException {
        if (pluginName == null) {
            throw new UserException(ExitCodes.USAGE, "plugin name is required");
        }

        // first make sure nothing extends this plugin
        List<String> usedBy = new ArrayList<>();
        Set<PluginsService.Bundle> bundles = PluginsService.getPluginBundles(env.pluginsFile());
        for (PluginsService.Bundle bundle : bundles) {
            for (String extendedPlugin : bundle.plugin.getExtendedPlugins()) {
                if (extendedPlugin.equals(pluginName)) {
                    usedBy.add(bundle.plugin.getName());
                }
            }
        }
        if (usedBy.isEmpty() == false) {
            throw new UserException(PLUGIN_STILL_USED, "plugin [" + pluginName + "] cannot be removed" +
                " because it is extended by other plugins: " + usedBy);
        }

        final Path pluginDir = env.pluginsFile().resolve(pluginName);
        final Path pluginConfigDir = env.configFile().resolve(pluginName);
        final Path removing = env.pluginsFile().resolve(".removing-" + pluginName);

        terminal.println("-> removing [" + pluginName + "]...");
        /*
         * If the plugin does not exist and the plugin config does not exist, fail to the user that the plugin is not found, unless there's
         * a marker file left from a previously failed attempt in which case we proceed to clean up the marker file. Or, if the plugin does
         * not exist, the plugin config does, and we are not purging, again fail to the user that the plugin is not found.
         */
        if ((!Files.exists(pluginDir) && !Files.exists(pluginConfigDir) && !Files.exists(removing))
                || (!Files.exists(pluginDir) && Files.exists(pluginConfigDir) && !purge)) {
            final String message = String.format(
                    Locale.ROOT, "plugin [%s] not found; run 'elasticsearch-plugin list' to get list of installed plugins", pluginName);
            throw new UserException(ExitCodes.CONFIG, message);
        }

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

        final Path pluginBinDir = env.binFile().resolve(pluginName);
        if (Files.exists(pluginBinDir)) {
            if (!Files.isDirectory(pluginBinDir)) {
                throw new UserException(ExitCodes.IO_ERROR, "bin dir for " + pluginName + " is not a directory");
            }
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
                        pluginConfigDir);
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

        IOUtils.rm(pluginPaths.toArray(new Path[pluginPaths.size()]));
    }

}
