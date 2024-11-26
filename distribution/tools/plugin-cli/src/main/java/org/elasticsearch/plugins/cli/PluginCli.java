/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A cli tool for adding, removing and listing plugins for elasticsearch.
 */
class PluginCli extends MultiCommand {

    private final Collection<Command> commands;

    PluginCli() {
        super("A tool for managing installed elasticsearch plugins");
        subcommands.put("list", new ListPluginsCommand());
        subcommands.put("install", new InstallPluginCommand());
        subcommands.put("remove", new RemovePluginCommand());
        commands = Collections.unmodifiableCollection(subcommands.values());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(commands);
    }

}
