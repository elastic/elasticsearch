/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import joptsimple.OptionSet;

import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CommandTests extends CommandTestCase {

    DummyCommand command;

    @Before
    public void setupCommand() {
        command = new DummyCommand();
    }

    @Override
    protected Command newCommand() {
        return command;
    }

    static class DummyCommand extends Command {
        boolean executed = false;
        Exception exception = null;

        DummyCommand() {
            super("Does nothing");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
            if (exception != null) {
                throw exception;
            }
            terminal.println("Normal output");
            terminal.println(Terminal.Verbosity.SILENT, "Silent output");
            terminal.println(Terminal.Verbosity.VERBOSE, "Verbose output");
            executed = true;
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Some extra help");
        }

    }

    public void testHelp() throws Exception {
        int status = executeMain("-h");
        String output = terminal.getOutput();
        assertEquals(output, ExitCodes.OK, status);
        assertTrue(output, output.contains("Does nothing"));
        assertTrue(output, output.contains("Some extra help"));
        assertFalse(command.executed);
    }

    public void testLongHelp() throws Exception {
        int status = executeMain("--help");
        String output = terminal.getOutput();
        assertEquals(output, ExitCodes.OK, status);
        assertTrue(output, output.contains("Does nothing"));
        assertTrue(output, output.contains("Some extra help"));
        assertFalse(command.executed);
    }

    public void testUnknownOptions() throws Exception {
        int status = executeMain("-Z");
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertTrue(error, error.contains("Does nothing"));
        assertFalse(output, output.contains("Some extra help")); // extra help not printed for usage errors
        assertTrue(error, error.contains("ERROR: Z is not a recognized option"));
        assertFalse(command.executed);
    }

    public void testLongUnknownOption() throws Exception {
        int status = executeMain("--foobar");
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertTrue(error, error.contains("Does nothing"));
        assertFalse(output, output.contains("Some extra help")); // extra help not printed for usage errors
        assertThat(error, containsString("ERROR: foobar is not a recognized option"));
        assertFalse(command.executed);
    }

    public void testVerbositySilentAndVerbose() throws Exception {
        int status = executeMain("-v", "-s");
        assertThat(status, equalTo(ExitCodes.USAGE));
        assertThat(terminal.getErrorOutput(), containsString("Option(s) [v/verbose] are unavailable"));
    }

    public void testSilentVerbosity() throws Exception {
        executeMain("-s");
        assertThat(terminal.getOutput(), containsString("Silent output"));
    }

    public void testNormalVerbosity() throws Exception {
        executeMain();
        assertThat(terminal.getOutput(), containsString("Normal output"));
    }

    public void testVerboseVerbosity() throws Exception {
        executeMain("-v");
        assertThat(terminal.getOutput(), containsString("Verbose output"));
    }

    public void testUserError() throws Exception {
        command.exception = new UserException(ExitCodes.DATA_ERROR, "Bad input");
        int status = executeMain();
        String output = terminal.getOutput();
        assertThat(output, status, equalTo(ExitCodes.DATA_ERROR));
        assertThat(terminal.getErrorOutput(), containsString("ERROR: Bad input"));
    }

    public void testUsageError() throws Exception {
        command.exception = new UserException(ExitCodes.USAGE, "something was no good");
        int status = executeMain();
        String output = terminal.getOutput();
        String error = terminal.getErrorOutput();
        assertEquals(output, ExitCodes.USAGE, status);
        assertThat(error, containsString("Does nothing"));
        assertThat(error, containsString("ERROR: something was no good"));
    }

}
