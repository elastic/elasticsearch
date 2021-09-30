/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.cli.action.InstallPluginAction;
import org.elasticsearch.plugins.cli.action.InstallPluginException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A command for the plugin cli to install a plugin into elasticsearch.
 * <p>
 * The install command takes a plugin id, which may be any of the following:
 * <ul>
 * <li>An official elasticsearch plugin name</li>
 * <li>Maven coordinates to a plugin zip</li>
 * <li>A URL to a plugin zip</li>
 * </ul>
 * <p>
 * Plugins are packaged as zip files. Each packaged plugin must contain a plugin properties file.
 * See {@link PluginInfo}.
 * <p>
 * The installation process first extracts the plugin files into a temporary
 * directory in order to verify the plugin satisfies the following requirements:
 * <ul>
 * <li>Jar hell does not exist, either between the plugin's own jars, or with elasticsearch</li>
 * <li>The plugin is not a module already provided with elasticsearch</li>
 * <li>If the plugin contains extra security permissions, the policy file is validated</li>
 * </ul>
 * <p>
 * A plugin may also contain an optional {@code bin} directory which contains scripts. The
 * scripts will be installed into a subdirectory of the elasticsearch bin directory, using
 * the name of the plugin, and the scripts will be marked executable.
 * <p>
 * A plugin may also contain an optional {@code config} directory which contains configuration
 * files specific to the plugin. The config files be installed into a subdirectory of the
 * elasticsearch config directory, using the name of the plugin. If any files to be installed
 * already exist, they will be skipped.
 */
class InstallPluginCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> batchOption;
    private final OptionSpec<String> arguments;

    // exit codes for install
    /**
     * A plugin with the same name is already installed.
     */
    static final int PLUGIN_EXISTS = 1;
    /**
     * The plugin zip is not properly structured.
     */
    static final int PLUGIN_MALFORMED = 2;

    InstallPluginCommand() {
        super("Install a plugin");
        this.batchOption = parser.acceptsAll(
            Arrays.asList("b", "batch"),
            "Enable batch mode explicitly, automatic confirmation of security permission"
        );
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("The following official plugins may be installed by name:");
        for (String plugin : InstallPluginAction.OFFICIAL_PLUGINS) {
            terminal.println("  " + plugin);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Path pluginsConfig = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(pluginsConfig)) {
            throw new UserException(
                ExitCodes.USAGE,
                "Plugins config [" + pluginsConfig + "] exists, please use [elasticsearch-plugin sync] instead"
            );
        }

        List<PluginDescriptor> plugins = arguments.values(options)
            .stream()
            // We only have one piece of data, which could be an ID or could be a location, so we use it for both
            .map(id -> new PluginDescriptor(id, id))
            .collect(Collectors.toList());
        final boolean isBatch = options.has(batchOption);

        if (plugins.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "at least one plugin ID is required");
        }

        InstallPluginAction action = new InstallPluginAction(terminal, env, isBatch);
        try {
            action.execute(plugins);
        } catch (InstallPluginException e) {
            int exitCode;

            switch (e.getProblem()) {
                case DUPLICATE_PLUGIN_ID:
                case UNKNOWN_PLUGIN:
                default:
                    exitCode = ExitCodes.USAGE;
                    break;

                case NO_XPACK:
                case RELEASE_SNAPSHOT_MISMATCH:
                    exitCode = ExitCodes.CONFIG;
                    break;

                case INVALID_CHECKSUM:
                case MISSING_CHECKSUM:
                    exitCode = ExitCodes.IO_ERROR;
                    break;

                case INVALID_SIGNATURE:
                    exitCode = ExitCodes.DATA_ERROR;
                    break;

                case PLUGIN_MALFORMED:
                    exitCode = PLUGIN_MALFORMED;
                    break;

                case PLUGIN_EXISTS:
                    exitCode = PLUGIN_EXISTS;
                    break;

                case INCOMPATIBLE_LICENSE:
                    exitCode = ExitCodes.NOPERM;
                    break;

                case INSTALLATION_FAILED:
                    exitCode = 1;
                    break;
            }

            throw new UserException(exitCode, e.getMessage(), e);
        }
    }
}
