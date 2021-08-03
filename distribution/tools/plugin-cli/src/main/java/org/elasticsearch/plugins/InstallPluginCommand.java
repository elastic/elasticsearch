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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
        final Path pluginsDescriptor = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(pluginsDescriptor)) {
            throw new UserException(ExitCodes.USAGE, "Plugins descriptor [" + pluginsDescriptor + "] exists, please use [elasticsearch-plugin sync] instead");
        }

        List<PluginDescriptor> plugins = arguments.values(options)
            .stream()
            .map(id -> new PluginDescriptor(id, id))
            .collect(Collectors.toList());
        final boolean isBatch = options.has(batchOption);

        InstallPluginAction action = new InstallPluginAction(terminal, env, isBatch);
        action.execute(plugins);
    }
}
