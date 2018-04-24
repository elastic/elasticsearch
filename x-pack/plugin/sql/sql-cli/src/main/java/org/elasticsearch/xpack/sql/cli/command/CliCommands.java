/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;

import java.util.Arrays;
import java.util.List;

/**
 * Wrapper for several commands
 */
public class CliCommands implements CliCommand {

    private final List<CliCommand> commands;

    public CliCommands(CliCommand... commands) {
        this.commands = Arrays.asList(commands);
    }

    @Override
    public boolean handle(CliTerminal terminal, CliSession cliSession, String line) {
        for (CliCommand cliCommand : commands) {
            if (cliCommand.handle(terminal, cliSession, line)) {
                return true;
            }
        }
        return false;
    }
}
