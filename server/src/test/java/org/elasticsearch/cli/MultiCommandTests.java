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

import joptsimple.OptionSet;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiCommandTests extends CommandTestCase {

    static class DummyMultiCommand extends MultiCommand {

        final AtomicBoolean closed = new AtomicBoolean();

        DummyMultiCommand() {
            super("A dummy multi command", () -> {
            });
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (this.closed.compareAndSet(false, true) == false) {
                throw new IllegalStateException("DummyMultiCommand already closed");
            }
        }
    }

    static class DummySubCommand extends Command {
        final boolean throwsExceptionOnClose;
        final AtomicBoolean closeCalled = new AtomicBoolean();

        DummySubCommand() {
            this(false);
        }

        DummySubCommand(final boolean throwsExceptionOnClose) {
            super("A dummy subcommand", () -> {
            });
            this.throwsExceptionOnClose = throwsExceptionOnClose;
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            terminal.println("Arguments: " + options.nonOptionArguments().toString());
        }

        @Override
        public void close() throws IOException {
            if (this.closeCalled.compareAndSet(false, true) == false) {
                throw new IllegalStateException("DummySubCommand already closed");
            }
            if (throwsExceptionOnClose) {
                throw new IOException("Error occurred while closing DummySubCommand");
            }
        }
    }

    DummyMultiCommand multiCommand;

    @Before
    public void setupCommand() {
        multiCommand = new DummyMultiCommand();
    }

    @Override
    protected Command newCommand() {
        return multiCommand;
    }

    public void testNoCommandsConfigured() throws Exception {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            execute();
        });
        assertEquals("No subcommands configured", e.getMessage());
    }

    public void testUnknownCommand() throws Exception {
        multiCommand.subcommands.put("something", new DummySubCommand());
        UserException e = expectThrows(UserException.class, () -> {
            execute("somethingelse");
        });
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertEquals("Unknown command [somethingelse]", e.getMessage());
    }

    public void testMissingCommand() throws Exception {
        multiCommand.subcommands.put("command1", new DummySubCommand());
        UserException e = expectThrows(UserException.class, () -> {
            execute();
        });
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertEquals("Missing command", e.getMessage());
    }

    public void testHelp() throws Exception {
        multiCommand.subcommands.put("command1", new DummySubCommand());
        multiCommand.subcommands.put("command2", new DummySubCommand());
        execute("-h");
        String output = terminal.getOutput();
        assertTrue(output, output.contains("command1"));
        assertTrue(output, output.contains("command2"));
    }

    public void testSubcommandHelp() throws Exception {
        multiCommand.subcommands.put("command1", new DummySubCommand());
        multiCommand.subcommands.put("command2", new DummySubCommand());
        execute("command2", "-h");
        String output = terminal.getOutput();
        assertFalse(output, output.contains("command1"));
        assertTrue(output, output.contains("A dummy subcommand"));
    }

    public void testSubcommandArguments() throws Exception {
        multiCommand.subcommands.put("command1", new DummySubCommand());
        execute("command1", "foo", "bar");
        String output = terminal.getOutput();
        assertFalse(output, output.contains("command1"));
        assertTrue(output, output.contains("Arguments: [foo, bar]"));
    }

    public void testClose() throws Exception {
        DummySubCommand subCommand1 = new DummySubCommand();
        DummySubCommand subCommand2 = new DummySubCommand();
        multiCommand.subcommands.put("command1", subCommand1);
        multiCommand.subcommands.put("command2", subCommand2);
        multiCommand.close();
        assertTrue("MultiCommand was not closed when close method is invoked", multiCommand.closed.get());
        assertTrue("SubCommand1 was not closed when close method is invoked", subCommand1.closeCalled.get());
        assertTrue("SubCommand2 was not closed when close method is invoked", subCommand2.closeCalled.get());
    }

    public void testCloseWhenSubCommandCloseThrowsException() throws Exception {
        final boolean command1Throws = randomBoolean();
        final boolean command2Throws = randomBoolean();
        final DummySubCommand subCommand1 = new DummySubCommand(command1Throws);
        final DummySubCommand subCommand2 = new DummySubCommand(command2Throws);
        multiCommand.subcommands.put("command1", subCommand1);
        multiCommand.subcommands.put("command2", subCommand2);
        if (command1Throws || command2Throws) {
            // verify exception is thrown, as well as other non failed sub-commands closed
            // properly.
            IOException ioe = expectThrows(IOException.class, multiCommand::close);
            assertEquals("Error occurred while closing DummySubCommand", ioe.getMessage());
            if (command1Throws && command2Throws) {
                assertEquals(1, ioe.getSuppressed().length);
                assertTrue("Missing suppressed exceptions", ioe.getSuppressed()[0] instanceof IOException);
                assertEquals("Error occurred while closing DummySubCommand", ioe.getSuppressed()[0].getMessage());
            }
        } else {
            multiCommand.close();
        }
        assertTrue("SubCommand1 was not closed when close method is invoked", subCommand1.closeCalled.get());
        assertTrue("SubCommand2 was not closed when close method is invoked", subCommand2.closeCalled.get());
    }

}
