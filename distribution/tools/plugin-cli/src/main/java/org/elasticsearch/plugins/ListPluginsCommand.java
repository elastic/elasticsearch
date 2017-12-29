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
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A command for the plugin cli to list plugins installed in elasticsearch.
 */
class ListPluginsCommand extends EnvironmentAwareCommand {

    ListPluginsCommand() {
        super("Lists installed elasticsearch plugins");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (Files.exists(env.pluginsFile()) == false) {
            throw new IOException("Plugins directory missing: " + env.pluginsFile());
        }

        terminal.println(Terminal.Verbosity.VERBOSE, "Plugins directory: " + env.pluginsFile());
        final List<Path> plugins = new ArrayList<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(env.pluginsFile())) {
            for (Path plugin : paths) {
                plugins.add(plugin);
            }
        }
        Collections.sort(plugins);
        for (final Path plugin : plugins) {
            if (UberPluginInfo.isUberPlugin(plugin)) {
                UberPluginInfo info = UberPluginInfo.readFromProperties(plugin);
                Set<String> subPluginNames = Arrays.stream(info.getPlugins()).collect(Collectors.toSet());
                List<Path> subPluginPaths = new ArrayList<>();
                try (DirectoryStream<Path> subPaths = Files.newDirectoryStream(plugin)) {
                    for (Path subPlugin : subPaths) {
                        if (subPluginNames.contains(subPlugin.getFileName().toString())) {
                            subPluginPaths.add(subPlugin);
                        }
                    }
                }
                Collections.sort(subPluginPaths);
                for (Path subPlugin : subPluginPaths) {
                    printPlugin(env, terminal, subPlugin, info.getName());
                }
            } else {
                printPlugin(env, terminal, plugin, null);
            }
        }
    }

    private void printPlugin(Environment env, Terminal terminal, Path plugin, @Nullable String uberPlugin) throws IOException {
        String name = (uberPlugin != null ? uberPlugin + ":" : "") + plugin.getFileName().toString();
        terminal.println(Terminal.Verbosity.SILENT, name);
        try {
            PluginInfo info = PluginInfo.readFromProperties(uberPlugin, env.pluginsFile().resolve(plugin.toAbsolutePath()));
            terminal.println(Terminal.Verbosity.VERBOSE, info.toString());
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("incompatible with version")) {
                terminal.println("WARNING: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }
}
