/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import joptsimple.OptionException;
import joptsimple.OptionSet;

import org.elasticsearch.test.ESTestCase;

public class CommandTests extends ESTestCase {

    static class UserErrorCommand extends Command {

        UserErrorCommand() {
            super("Throws a user error", () -> {});
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            throw new UserException(ExitCodes.DATA_ERROR, "Bad input");
        }

        @Override
        protected boolean addShutdownHook() {
            return false;
        }

    }

    static class UsageErrorCommand extends Command {

        UsageErrorCommand() {
            super("Throws a usage error", () -> {});
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            throw new UserException(ExitCodes.USAGE, "something was no good");
        }

        @Override
        protected boolean addShutdownHook() {
            return false;
        }

    }

    static class NoopCommand extends Command {

        boolean executed = false;

        NoopCommand() {
            super("Does nothing", () -> {});
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            terminal.println("Normal output");
            terminal.println(Terminal.Verbosity.SILENT, "Silent output");
            terminal.println(Terminal.Verbosity.VERBOSE, "Verbose output");
            executed = true;
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Some extra help");
        }

        @Override
        protected boolean addShutdownHook() {
            return false;
        }

    }

    public void testHelp() throws Exception {
        NoopCommand command = new NoopCommand();
        MockTerminal terminal = new MockTerminal();
        String[] args = { "-h" };
        int status = command.main(args, terminal);
        String output = terminal.getOutput();
        assertEquals(output, ExitCodes.OK, status);
        assertTrue(output, output.contains("Does nothing"));
        assertTrue(output, output.contains("Some extra help"));
        assertFalse(command.executed);

        command = new NoopCommand();
        String[] args2 = { "--help" };
        status = command.main(args2, terminal);
        output = terminal.getOutput();
        assertEquals(output, ExitCodes.OK, status);
        assertTrue(output, output.contains("Does nothing"));
        assertTrue(output, output.contains("Some extra help"));
        assertFalse(command.executed);
    }

    public void testUnknownOptions() throws Exception {
        NoopCommand command = new NoopCommand();
        MockTerminal terminal = new MockTerminal();
        String[] args = { "-Z" };
        int status = command.main(args, terminal);
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertTrue(error, error.contains("Does nothing"));
        assertFalse(output, output.contains("Some extra help")); // extra help not printed for usage errors
        assertTrue(error, error.contains("ERROR: Z is not a recognized option"));
        assertFalse(command.executed);

        command = new NoopCommand();
        String[] args2 = { "--foobar" };
        status = command.main(args2, terminal);
        output = terminal.getOutput();
        error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertTrue(error, error.contains("Does nothing"));
        assertFalse(output, output.contains("Some extra help")); // extra help not printed for usage errors
        assertTrue(error, error.contains("ERROR: Z is not a recognized option"));
        assertFalse(command.executed);
    }

    public void testVerbositySilentAndVerbose() throws Exception {
        MockTerminal terminal = new MockTerminal();
        NoopCommand command = new NoopCommand();
        String[] args = { "-v", "-s" };
        OptionException e = expectThrows(OptionException.class, () -> { command.mainWithoutErrorHandling(args, terminal); });
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("Option(s) [v/verbose] are unavailable given other options on the command line")
        );
    }

    public void testSilentVerbosity() throws Exception {
        MockTerminal terminal = new MockTerminal();
        NoopCommand command = new NoopCommand();
        String[] args = { "-s" };
        command.main(args, terminal);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("Silent output"));
    }

    public void testNormalVerbosity() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        NoopCommand command = new NoopCommand();
        String[] args = {};
        command.main(args, terminal);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("Normal output"));
    }

    public void testVerboseVerbosity() throws Exception {
        MockTerminal terminal = new MockTerminal();
        NoopCommand command = new NoopCommand();
        String[] args = { "-v" };
        command.main(args, terminal);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("Verbose output"));
    }

    public void testUserError() throws Exception {
        MockTerminal terminal = new MockTerminal();
        UserErrorCommand command = new UserErrorCommand();
        String[] args = {};
        int status = command.main(args, terminal);
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.DATA_ERROR, status);
        assertTrue(error, error.contains("ERROR: Bad input"));
    }

    public void testUsageError() throws Exception {
        MockTerminal terminal = new MockTerminal();
        UsageErrorCommand command = new UsageErrorCommand();
        String[] args = {};
        int status = command.main(args, terminal);
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertTrue(error, error.contains("Throws a usage error"));
        assertTrue(error, error.contains("ERROR: something was no good"));
    }

}
