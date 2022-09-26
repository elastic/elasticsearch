/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
     * Construct the multi-command with the specified command description and runnable to execute before main is invoked.
     *  @param description the multi-command description
     *
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
