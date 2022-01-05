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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

/**
 * An action to execute within a cli.
 */
public abstract class Command implements Closeable {

    /** A description of the command, used in the help output. */
    protected final String description;

    private final Runnable beforeMain;

    /** The option parser for this command. */
    protected final OptionParser parser = new OptionParser();

    private final OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "Show help").forHelp();
    private final OptionSpec<Void> silentOption = parser.acceptsAll(Arrays.asList("s", "silent"), "Show minimal output");
    private final OptionSpec<Void> verboseOption = parser.acceptsAll(Arrays.asList("v", "verbose"), "Show verbose output")
        .availableUnless(silentOption);

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked.
     *
     * @param description the command description
     * @param beforeMain the before-main runnable
     */
    public Command(final String description, final Runnable beforeMain) {
        this.description = description;
        this.beforeMain = beforeMain;
    }

    private Thread shutdownHookThread;

    /** Parses options for this command from args and executes it. */
    public final int main(String[] args, Terminal terminal) throws Exception {
        if (addShutdownHook()) {

            shutdownHookThread = new Thread(() -> {
                try {
                    this.close();
                } catch (final IOException e) {
                    try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
                        e.printStackTrace(pw);
                        terminal.errorPrintln(sw.toString());
                    } catch (final IOException impossible) {
                        // StringWriter#close declares a checked IOException from the Closeable interface but the Javadocs for StringWriter
                        // say that an exception here is impossible
                        throw new AssertionError(impossible);
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }

        beforeMain.run();

        try {
            mainWithoutErrorHandling(args, terminal);
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
    protected void mainWithoutErrorHandling(String[] args, Terminal terminal) throws Exception {
        final OptionSet options = parser.parse(args);

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

        execute(terminal, options);
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
            terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
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
    protected abstract void execute(Terminal terminal, OptionSet options) throws Exception;

    /**
     * Return whether or not to install the shutdown hook to cleanup resources on exit. This method should only be overridden in test
     * classes.
     *
     * @return whether or not to install the shutdown hook
     */
    protected boolean addShutdownHook() {
        return true;
    }

    /** Gets the shutdown hook thread if it exists **/
    Thread getShutdownHookThread() {
        return shutdownHookThread;
    }

    @Override
    public void close() throws IOException {

    }

}
