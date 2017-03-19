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

import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin cli to remove a plugin from elasticsearch.
 */
class RemovePluginCommand extends EnvironmentAwareCommand {

    private final OptionSpec<String> arguments;

    RemovePluginCommand() {
        super("Removes a plugin from elasticsearch");
        this.arguments = parser.nonOptions("plugin name");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        String arg = arguments.value(options);
        execute(terminal, arg, env);
    }

    /**
    * Remove the plugin named {@code plugin named}.
    *
    * @param terminal       the Terminal to use for input/output
    * @param pluginName     the name of the plugin to remove.
    * @param env            the environment for the local node, which may be used for the local settings and path
    *
    * @throws UserException if plugin name is null.
    * @throws UserException if plugin directory does not exist. Returns command usage info on printing the plugin list
    * @throws UserException if plugin bin directory does not exist
    * @throws OIException   if the plugin is no longer available to delete or permission is denied.
    */
    void execute(Terminal terminal, String pluginName, Environment env) throws Exception {
        if (pluginName == null) {
            throw new UserException(ExitCodes.USAGE, "plugin name is required");
        }

        terminal.println("-> Removing " + Strings.coalesceToEmpty(pluginName) + "...");

        final Path pluginDir = env.pluginsFile().resolve(pluginName);
        if (Files.exists(pluginDir) == false) {
            throw new UserException(
                    ExitCodes.CONFIG,
                    "plugin " + pluginName + " not found; run 'elasticsearch-plugin list' to get list of installed plugins");
        }

        final List<Path> pluginPaths = new ArrayList<>();

        final Path pluginBinDir = env.binFile().resolve(pluginName);
        if (Files.exists(pluginBinDir)) {
            if (Files.isDirectory(pluginBinDir) == false) {
                throw new UserException(ExitCodes.IO_ERROR, "Bin dir for " + pluginName + " is not a directory");
            }
            pluginPaths.add(pluginBinDir);
            terminal.println(VERBOSE, "Removing: " + pluginBinDir);
        }

        terminal.println(VERBOSE, "Removing: " + pluginDir);
        final Path tmpPluginDir = env.pluginsFile().resolve(".removing-" + pluginName);
        try {
            Files.move(pluginDir, tmpPluginDir, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException e) {
            // this can happen on a union filesystem when a plugin is not installed on the top layer; we fall back to a non-atomic move
            Files.move(pluginDir, tmpPluginDir);
        }
        pluginPaths.add(tmpPluginDir);

        IOUtils.rm(pluginPaths.toArray(new Path[pluginPaths.size()]));

        // we preserve the config files in case the user is upgrading the plugin, but we print
        // a message so the user knows in case they want to remove manually
        final Path pluginConfigDir = env.configFile().resolve(pluginName);
        if (Files.exists(pluginConfigDir)) {
            terminal.println(
                    "-> Preserving plugin config files [" + pluginConfigDir + "] in case of upgrade, delete manually if not needed");
        }
    }

}
