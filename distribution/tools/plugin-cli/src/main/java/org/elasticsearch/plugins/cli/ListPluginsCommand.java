/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import joptsimple.OptionSet;

import org.elasticsearch.Build;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginDescriptor;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.plugins.cli.SyncPluginsAction.ELASTICSEARCH_PLUGINS_YML_CACHE;

/**
 * A command for the plugin cli to list plugins installed in elasticsearch.
 */
class ListPluginsCommand extends EnvironmentAwareCommand {

    ListPluginsCommand() {
        super("Lists installed elasticsearch plugins");
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        if (Files.exists(env.pluginsDir()) == false) {
            throw new IOException("Plugins directory missing: " + env.pluginsDir());
        }

        terminal.println(Terminal.Verbosity.VERBOSE, "Plugins directory: " + env.pluginsDir());
        final List<Path> plugins = new ArrayList<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(env.pluginsDir())) {
            for (Path path : paths) {
                if (path.getFileName().toString().equals(ELASTICSEARCH_PLUGINS_YML_CACHE) == false) {
                    plugins.add(path);
                }
            }
        }
        Collections.sort(plugins);
        for (final Path plugin : plugins) {
            printPlugin(env, terminal, plugin, "");
        }
    }

    private static void printPlugin(Environment env, Terminal terminal, Path plugin, String prefix) throws IOException {
        terminal.println(Terminal.Verbosity.SILENT, prefix + plugin.getFileName().toString());
        PluginDescriptor info = PluginDescriptor.readFromProperties(env.pluginsDir().resolve(plugin));
        terminal.println(Terminal.Verbosity.VERBOSE, info.toString(prefix));

        // When PluginDescriptor#getElasticsearchVersion returns a string, we can revisit the need
        // for a semantic version
        String semanticVersion = InstallPluginAction.getSemanticVersion(Build.current().version());
        String buildVersion = Objects.nonNull(semanticVersion) ? semanticVersion : Build.current().version();
        if (info.getElasticsearchVersion().toString().equals(buildVersion) == false) {
            terminal.errorPrintln(
                "WARNING: plugin ["
                    + info.getName()
                    + "] was built for Elasticsearch version "
                    + info.getElasticsearchVersion()
                    + " but version "
                    + buildVersion
                    + " is required"
            );
        }
    }
}
