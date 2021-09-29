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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.cli.action.RemovePluginAction;
import org.elasticsearch.plugins.cli.action.RemovePluginAction.RemovePluginProblem;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        final List<PluginDescriptor> plugins = arguments.values(options).stream().map(PluginDescriptor::new).collect(Collectors.toList());

        final RemovePluginAction action = new RemovePluginAction(new TerminalLogger(terminal), env, options.has(purgeOption));

        final Tuple<RemovePluginProblem, String> problem = action.checkRemovePlugins(plugins);
        if (problem != null) {
            int exitCode;
            switch (problem.v1()) {
                case NOT_FOUND:
                    exitCode = ExitCodes.CONFIG;
                    break;

                case STILL_USED:
                    exitCode = PLUGIN_STILL_USED;
                    break;

                case BIN_FILE_NOT_DIRECTORY:
                    exitCode = ExitCodes.IO_ERROR;
                    break;

                default:
                    exitCode = ExitCodes.USAGE;
                    break;
            }

            throw new UserException(exitCode, problem.v2());
        }

        action.removePlugins(plugins);
    }
}
