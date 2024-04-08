/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * An action to execute within a cli.
 */
public abstract class Command implements Closeable {

    /** A description of the command, used in the help output. */
    protected final String description;

    /** The option parser for this command. */
    protected final OptionParser parser = new OptionParser();

    private final OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "Show help").forHelp();
    private final OptionSpec<Void> silentOption = parser.acceptsAll(Arrays.asList("s", "silent"), "Show minimal output");
    private final OptionSpec<Void> verboseOption = parser.acceptsAll(Arrays.asList("v", "verbose"), "Show verbose output")
        .availableUnless(silentOption);

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked.
     *  @param description the command description
     *
     */
    public Command(final String description) {
        this.description = description;
    }

    /** Parses options for this command from args and executes it. */
    public final int main(String[] args, Terminal terminal, ProcessInfo processInfo) throws Exception {
        try {
            mainWithoutErrorHandling(args, terminal, processInfo);
        } catch (OptionException e) {
            // print help to stderr on exceptions
            printHelp(terminal, true);
            terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
            return ExitCodes.USAGE;
        } catch (UserException e) {
            if (e.exitCode == ExitCodes.USAGE) {
                printHelp(terminal, true);
            }
            printUserException(terminal, e);
            return e.exitCode;
        }
        return ExitCodes.OK;
    }

    /**
     * Executes the command, but all errors are thrown.
     */
    protected void mainWithoutErrorHandling(String[] args, Terminal terminal, ProcessInfo processInfo) throws Exception {
        final OptionSet options = parseOptions(args);

        if (options.has(helpOption)) {
            printHelp(terminal, false);
            return;
        }

        if (options.has(silentOption)) {
            terminal.setVerbosity(Terminal.Verbosity.SILENT);
        } else if (options.has(verboseOption)) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        } else {
            terminal.setVerbosity(Terminal.Verbosity.NORMAL);
        }

        execute(terminal, options, processInfo);
    }

    /**
     * Parse command line arguments for this command.
     * @param args The string arguments passed to the command
     * @return A set of parsed options
     */
    public OptionSet parseOptions(String[] args) {
        return parser.parse(args);
    }

    /** Prints a help message for the command to the terminal. */
    private void printHelp(Terminal terminal, boolean toStdError) throws IOException {
        if (toStdError) {
            terminal.errorPrintln(description);
            terminal.errorPrintln("");
            parser.printHelpOn(terminal.getErrorWriter());
        } else {
            terminal.println(description);
            terminal.println("");
            printAdditionalHelp(terminal);
            parser.printHelpOn(terminal.getWriter());
        }
    }

    /** Prints additional help information, specific to the command */
    protected void printAdditionalHelp(Terminal terminal) {}

    protected void printUserException(Terminal terminal, UserException e) {
        if (e.getMessage() != null) {
            terminal.errorPrintln("");
            terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage() + ", with exit code " + e.exitCode);
        }
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    protected static void exit(int status) {
        System.exit(status);
    }

    /**
     * Executes this command.
     *
     * Any runtime user errors (like an input file that does not exist), should throw a {@link UserException}. */
    protected abstract void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception;

    @Override
    public void close() throws IOException {

    }

}
