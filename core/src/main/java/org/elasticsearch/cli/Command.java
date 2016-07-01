/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.util.Arrays;

/**
 * An action to execute within a cli.
 */
public abstract class Command {

    /** A description of the command, used in the help output. */
    protected final String description;

    /** The option parser for this command. */
    protected final OptionParser parser = new OptionParser();

    private final OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "show help").forHelp();
    private final OptionSpec<Void> silentOption = parser.acceptsAll(Arrays.asList("s", "silent"), "show minimal output");
    private final OptionSpec<Void> verboseOption = parser.acceptsAll(Arrays.asList("v", "verbose"), "show verbose output");

    public Command(String description) {
        this.description = description;
    }

    /** Parses options for this command from args and executes it. */
    public final int main(String[] args, Terminal terminal) throws Exception {
        try {
            mainWithoutErrorHandling(args, terminal);
        } catch (OptionException e) {
            printHelp(terminal);
            terminal.println(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
            return ExitCodes.USAGE;
        } catch (UserError e) {
            if (e.exitCode == ExitCodes.USAGE) {
                printHelp(terminal);
            }
            terminal.println(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
            return e.exitCode;
        }
        return ExitCodes.OK;
    }

    /**
     * Executes the command, but all errors are thrown.
     */
    void mainWithoutErrorHandling(String[] args, Terminal terminal) throws Exception {
        final OptionSet options = parser.parse(args);

        if (options.has(helpOption)) {
            printHelp(terminal);
            return;
        }

        if (options.has(silentOption)) {
            if (options.has(verboseOption)) {
                // mutually exclusive, we can remove this with jopt-simple 5.0, which natively supports it
                throw new UserError(ExitCodes.USAGE, "Cannot specify -s and -v together");
            }
            terminal.setVerbosity(Terminal.Verbosity.SILENT);
        } else if (options.has(verboseOption)) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        } else {
            terminal.setVerbosity(Terminal.Verbosity.NORMAL);
        }

        execute(terminal, options);
    }

    /** Prints a help message for the command to the terminal. */
    private void printHelp(Terminal terminal) throws IOException {
        terminal.println(description);
        terminal.println("");
        printAdditionalHelp(terminal);
        parser.printHelpOn(terminal.getWriter());
    }

    /** Prints additional help information, specific to the command */
    protected void printAdditionalHelp(Terminal terminal) {}

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    protected static void exit(int status) {
        System.exit(status);
    }

    /**
     * Executes this command.
     *
     * Any runtime user errors (like an input file that does not exist), should throw a {@link UserError}. */
    protected abstract void execute(Terminal terminal, OptionSet options) throws Exception;

}
