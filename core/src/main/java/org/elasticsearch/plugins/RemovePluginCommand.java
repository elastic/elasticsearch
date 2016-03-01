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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.env.Environment;

import static org.elasticsearch.common.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin cli to remove a plugin from elasticsearch.
 */
class RemovePluginCommand extends Command {

    private final Environment env;
    private final OptionSpec<String> arguments;

    RemovePluginCommand(Environment env) {
        super("Removes a plugin from elasticsearch");
        this.env = env;
        this.arguments = parser.nonOptions("plugin name");
    }

    @Override
    public int execute(Terminal terminal, OptionSet options) throws Exception {
        // TODO: in jopt-simple 5.0 we can enforce a min/max number of positional args
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserError(ExitCodes.USAGE, "Must supply a single plugin id argument");
        }
        execute(terminal, args.get(0));
        return ExitCodes.OK;
    }

    // pkg private for testing
    void execute(Terminal terminal, String pluginName) throws Exception {
        terminal.println("-> Removing " + Strings.coalesceToEmpty(pluginName) + "...");

        Path pluginDir = env.pluginsFile().resolve(pluginName);
        if (Files.exists(pluginDir) == false) {
            throw new UserError(CliTool.ExitStatus.USAGE.status(), "Plugin " + pluginName + " not found. Run 'plugin list' to get list of installed plugins.");
        }

        List<Path> pluginPaths = new ArrayList<>();

        Path pluginBinDir = env.binFile().resolve(pluginName);
        if (Files.exists(pluginBinDir)) {
            if (Files.isDirectory(pluginBinDir) == false) {
                throw new UserError(CliTool.ExitStatus.IO_ERROR.status(), "Bin dir for " + pluginName + " is not a directory");
            }
            pluginPaths.add(pluginBinDir);
            terminal.println(VERBOSE, "Removing: " + pluginBinDir);
        }

        terminal.println(VERBOSE, "Removing: " + pluginDir);
        Path tmpPluginDir = env.pluginsFile().resolve(".removing-" + pluginName);
        Files.move(pluginDir, tmpPluginDir, StandardCopyOption.ATOMIC_MOVE);
        pluginPaths.add(tmpPluginDir);

        IOUtils.rm(pluginPaths.toArray(new Path[pluginPaths.size()]));
    }
}
