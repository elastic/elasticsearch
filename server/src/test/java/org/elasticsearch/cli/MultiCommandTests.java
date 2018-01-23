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

public class MultiCommandTests extends CommandTestCase {

    static class DummyMultiCommand extends MultiCommand {
        DummyMultiCommand() {
            super("A dummy multi command", () -> {});
        }
    }

    static class DummySubCommand extends Command {
        DummySubCommand() {
            super("A dummy subcommand", () -> {});
        }
        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            terminal.println("Arguments: " + options.nonOptionArguments().toString());
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
}
