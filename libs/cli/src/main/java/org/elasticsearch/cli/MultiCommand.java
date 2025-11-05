/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;

import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A cli tool which is made up of multiple subcommands.
 */
public class MultiCommand extends Command {

    protected final Map<String, Command> subcommands = new LinkedHashMap<>();

    private final NonOptionArgumentSpec<String> arguments = parser.nonOptions("command");
    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Constructs a multi-command with the specified description.
     *
     * <p>A MultiCommand is a CLI tool that contains multiple sub-commands, each
     * represented by a separate {@link Command} instance. The user specifies which
     * sub-command to run as the first argument.
     *
     * @param description the multi-command description to be displayed in help output
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * MultiCommand tool = new MultiCommand("Elasticsearch administration tool");
     * tool.subcommands.put("index", new IndexCommand());
     * tool.subcommands.put("cluster", new ClusterCommand());
     * }</pre>
     */
    public MultiCommand(final String description) {
        super(description);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
        parser.posixlyCorrect(true);
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        printSubCommandList(terminal::println);
    }

    @Override
    protected void printUserException(Terminal terminal, UserException e) {
        super.printUserException(terminal, e);
        if (e instanceof MissingCommandException) {
            terminal.errorPrintln("");
            printSubCommandList(terminal::errorPrintln);
        }
    }

    private void printSubCommandList(Consumer<String> println) {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }
        println.accept("Commands");
        println.accept("--------");
        for (Map.Entry<String, Command> subcommand : subcommands.entrySet()) {
            println.accept(subcommand.getKey() + " - " + subcommand.getValue().description);
        }
        println.accept("");
    }

    /**
     * Executes the appropriate sub-command based on the first command-line argument.
     *
     * <p>This method parses the first non-option argument to determine which sub-command
     * to execute, then delegates to that sub-command's {@link Command#mainWithoutErrorHandling(String[], Terminal, ProcessInfo)}
     * method.
     *
     * @param terminal the terminal for input/output operations
     * @param options the parsed command-line options
     * @param processInfo information about the current process
     * @throws Exception if an error occurs during sub-command execution
     * @throws MissingCommandException if no sub-command name is provided
     * @throws UserException if the specified sub-command does not exist
     * @throws IllegalStateException if no sub-commands have been configured
     */
    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }

        // .values(...) returns an unmodifiable list
        final List<String> args = new ArrayList<>(arguments.values(options));
        if (args.isEmpty()) {
            throw new MissingCommandException();
        }

        String subcommandName = args.remove(0);
        Command subcommand = subcommands.get(subcommandName);
        if (subcommand == null) {
            throw new UserException(ExitCodes.USAGE, "Unknown command [" + subcommandName + "]");
        }

        for (final KeyValuePair pair : this.settingOption.values(options)) {
            args.add("-E" + pair);
        }

        subcommand.mainWithoutErrorHandling(args.toArray(new String[0]), terminal, processInfo);
    }

    /**
     * Closes this multi-command and all of its sub-commands.
     *
     * <p>This method iterates through all registered sub-commands and closes each one,
     * ensuring proper resource cleanup.
     *
     * @throws IOException if an I/O error occurs while closing any sub-command
     */
    @Override
    public void close() throws IOException {
        IOUtils.close(subcommands.values());
    }

    static final class MissingCommandException extends UserException {
        MissingCommandException() {
            super(ExitCodes.USAGE, "Missing required command");
        }
    }
}
