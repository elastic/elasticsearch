/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
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
     * Constructs the command with the specified command description.
     *
     * @param description the command description to be displayed in help output
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * public class MyCommand extends Command {
     *     public MyCommand() {
     *         super("Performs custom processing");
     *     }
     * }
     * }</pre>
     */
    public Command(final String description) {
        this.description = description;
    }

    /**
     * Parses command-line options and executes this command with proper error handling.
     *
     * <p>This is the main entry point for command execution. It handles parsing
     * of command-line arguments, error handling, and returning appropriate exit codes.
     * All exceptions are caught and converted to appropriate exit codes.
     *
     * @param args the command-line arguments to parse
     * @param terminal the terminal for input/output operations
     * @param processInfo information about the current process (system properties, environment variables, etc.)
     * @return the exit code (0 for success, non-zero for errors as defined in {@link ExitCodes})
     * @throws IOException if an I/O error occurs during command execution
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * Command cmd = new MyCommand();
     * Terminal terminal = Terminal.DEFAULT;
     * ProcessInfo processInfo = ProcessInfo.fromSystem();
     * int exitCode = cmd.main(new String[]{"--verbose"}, terminal, processInfo);
     * }</pre>
     */
    public final int main(String[] args, Terminal terminal, ProcessInfo processInfo) throws IOException {
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
        } catch (IOException ioe) {
            terminal.errorPrintln(ioe);
            return ExitCodes.IO_ERROR;
        } catch (Throwable t) {
            // It's acceptable to catch Throwable at this point:
            // We're about to exit and only want to print the stacktrace with appropriate formatting (e.g. JSON).
            terminal.errorPrintln(t);
            return ExitCodes.CODE_ERROR;
        }
        return ExitCodes.OK;
    }

    /**
     * Executes the command without error handling, allowing all exceptions to propagate.
     *
     * <p>This method parses options, handles help and verbosity flags, and delegates
     * to {@link #execute(Terminal, OptionSet, ProcessInfo)}. Unlike {@link #main(String[], Terminal, ProcessInfo)},
     * this method does not catch exceptions, allowing callers to handle them.
     *
     * @param args the command-line arguments to parse
     * @param terminal the terminal for input/output operations
     * @param processInfo information about the current process
     * @throws Exception if any error occurs during command execution
     */
    protected void mainWithoutErrorHandling(String[] args, Terminal terminal, ProcessInfo processInfo) throws Exception {
        final OptionSet options = parseOptions(args);

        if (options.has(helpOption)) {
            printHelp(terminal, false);
            return;
        }

        LoggerFactory loggerFactory = LoggerFactory.provider();
        if (options.has(silentOption)) {
            terminal.setVerbosity(Terminal.Verbosity.SILENT);
            loggerFactory.setRootLevel(Level.OFF);
        } else if (options.has(verboseOption)) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
            loggerFactory.setRootLevel(Level.DEBUG);
        } else {
            terminal.setVerbosity(Terminal.Verbosity.NORMAL);
            loggerFactory.setRootLevel(Level.INFO);
        }

        execute(terminal, options, processInfo);
    }

    /**
     * Parses command-line arguments for this command using the configured option parser.
     *
     * @param args the string arguments passed to the command
     * @return a set of parsed options
     * @throws joptsimple.OptionException if the arguments cannot be parsed
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * OptionSet options = parseOptions(new String[]{"--verbose", "input.txt"});
     * }</pre>
     */
    public OptionSet parseOptions(String[] args) {
        return parser.parse(args);
    }

    /** Prints a help message for the command to the terminal. */
    private void printHelp(Terminal terminal, boolean toStdError) throws IOException {
        StringWriter writer = new StringWriter();
        parser.printHelpOn(writer);
        if (toStdError) {
            terminal.errorPrintln(description);
            terminal.errorPrintln("");
            terminal.errorPrintln(writer.toString());
        } else {
            terminal.println(description);
            terminal.println("");
            printAdditionalHelp(terminal);
            terminal.println(writer.toString());
        }
    }

    /**
     * Prints additional help information specific to this command.
     *
     * <p>Subclasses can override this method to provide command-specific help text
     * that will be displayed when the user requests help via the -h or --help option.
     *
     * @param terminal the terminal to write help output to
     */
    protected void printAdditionalHelp(Terminal terminal) {}

    /**
     * Prints a user exception message to the terminal's error stream.
     *
     * <p>Subclasses can override this method to customize how user exceptions are displayed.
     *
     * @param terminal the terminal to write error output to
     * @param e the user exception to print
     */
    protected void printUserException(Terminal terminal, UserException e) {
        if (e.getMessage() != null) {
            terminal.errorPrintln("");
            terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage() + ", with exit code " + e.exitCode);
        }
    }

    /**
     * Exits the JVM with the specified status code.
     *
     * <p>This method calls {@link System#exit(int)} and should be used sparingly,
     * typically only after {@link #main(String[], Terminal, ProcessInfo)} has completed.
     *
     * @param status the exit status code (0 for success, non-zero for errors)
     */
    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    protected static void exit(int status) {
        System.exit(status);
    }

    /**
     * Executes the core logic of this command.
     *
     * <p>Subclasses must implement this method to provide command-specific functionality.
     * This method is called by {@link #mainWithoutErrorHandling(String[], Terminal, ProcessInfo)}
     * after options have been parsed and help/verbosity flags processed.
     *
     * @param terminal the terminal for input/output operations
     * @param options the parsed command-line options
     * @param processInfo information about the current process
     * @throws Exception if any error occurs during execution
     * @throws UserException for user-correctable errors (e.g., invalid input file)
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * @Override
     * protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
     *     String input = options.valueOf(inputOption);
     *     terminal.println("Processing: " + input);
     *     // ... perform command logic ...
     * }
     * }</pre>
     */
    protected abstract void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception;

    /**
     * Closes this command and releases any resources.
     *
     * <p>The default implementation does nothing. Subclasses should override this method
     * to release any resources they have acquired.
     *
     * @throws IOException if an I/O error occurs while closing resources
     */
    @Override
    public void close() throws IOException {

    }

}
