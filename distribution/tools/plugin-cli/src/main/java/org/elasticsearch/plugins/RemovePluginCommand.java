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
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A command for the plugin CLI to remove plugins from Elasticsearch.
 */
class RemovePluginCommand extends EnvironmentAwareCommand {
    private final OptionSpec<Void> purgeOption;
    private final OptionSpec<String> arguments;

    RemovePluginCommand() {
        super("removes plugins from Elasticsearch");
        this.purgeOption = parser.acceptsAll(Arrays.asList("p", "purge"), "Purge plugin configuration files");
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        final Path pluginsDescriptor = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(pluginsDescriptor)) {
            throw new UserException(1, "Plugins descriptor [" + pluginsDescriptor + "] exists, please use [elasticsearch-plugin sync] instead");
        }

        final List<PluginDescriptor> plugins = arguments.values(options).stream().map(PluginDescriptor::new).collect(Collectors.toList());

        final RemovePluginAction action = new RemovePluginAction(terminal, env, options.has(purgeOption));
        action.execute(plugins);
    }
}
