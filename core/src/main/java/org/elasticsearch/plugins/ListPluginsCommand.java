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
import org.elasticsearch.cli.SettingCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A command for the plugin cli to list plugins installed in elasticsearch.
 */
class ListPluginsCommand extends SettingCommand {

    ListPluginsCommand() {
        super("Lists installed elasticsearch plugins");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Map<String, String> settings) throws Exception {
        final Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, terminal, settings);
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
            terminal.println(plugin.getFileName().toString());
            PluginInfo info = PluginInfo.readFromProperties(env.pluginsFile().resolve(plugin.toAbsolutePath()));
            terminal.println(Terminal.Verbosity.VERBOSE, info.toString());
        }
    }
}
