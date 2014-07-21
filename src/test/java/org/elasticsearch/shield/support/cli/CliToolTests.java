/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support.cli;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.shield.support.cli.CliToolConfig.Builder.cmd;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class CliToolTests extends CliToolTestCase {

    @Test
    public void testOK() throws Exception {
        Terminal terminal = new TerminalMock();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                executed.set(true);
                return CliTool.ExitStatus.OK;
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        int status = tool.execute();
        assertThat(executed.get(), is(true));
        assertThat(status, is(CliTool.ExitStatus.OK.status()));
    }

    @Test
    public void testUsageError() throws Exception {
        Terminal terminal = new TerminalMock();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                executed.set(true);
                return CliTool.ExitStatus.USAGE;
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        int status = tool.execute();
        assertThat(executed.get(), is(true));
        assertThat(status, is(CliTool.ExitStatus.USAGE.status()));
    }

    @Test
    public void testIOError() throws Exception {
        Terminal terminal = new TerminalMock();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new IOException("io error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        int status = tool.execute();
        assertThat(executed.get(), is(true));
        assertThat(status, is(CliTool.ExitStatus.IO_ERROR.status()));
    }

    @Test
    public void testCodeError() throws Exception {
        Terminal terminal = new TerminalMock();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new Exception("random error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        int status = tool.execute();
        assertThat(executed.get(), is(true));
        assertThat(status, is(CliTool.ExitStatus.CODE_ERROR.status()));
    }

    public void testMultiCommand() {
        Terminal terminal = new TerminalMock();
        int count = randomIntBetween(2, 7);
        final AtomicReference<Boolean>[] executed = new AtomicReference[count];
        for (int i = 0; i < executed.length; i++) {
            executed[i] = new AtomicReference<>(false);
        }
        NamedCommand[] cmds = new NamedCommand[count];
        for (int i = 0; i < count; i++) {
            final int index = i;
            cmds[i] = new NamedCommand("cmd" + index, terminal) {
                @Override
                public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                    executed[index].set(true);
                    return CliTool.ExitStatus.OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int cmdIndex = randomIntBetween(0, count-1);
        int status = tool.execute("cmd" + cmdIndex);
        assertThat(status, is(CliTool.ExitStatus.OK.status()));
        for (int i = 0; i < executed.length; i++) {
            assertThat(executed[i].get(), is(i == cmdIndex));
        }
    }

    public void testMultiCommand_UnknownCommand() {
        Terminal terminal = new TerminalMock();
        int count = randomIntBetween(2, 7);
        final AtomicReference<Boolean>[] executed = new AtomicReference[count];
        for (int i = 0; i < executed.length; i++) {
            executed[i] = new AtomicReference<>(false);
        }
        NamedCommand[] cmds = new NamedCommand[count];
        for (int i = 0; i < count; i++) {
            final int index = i;
            cmds[i] = new NamedCommand("cmd" + index, terminal) {
                @Override
                public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                    executed[index].set(true);
                    return CliTool.ExitStatus.OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int status = tool.execute("cmd" + count); // "cmd" + count doesn't exist
        assertThat(status, is(CliTool.ExitStatus.USAGE.status()));
        for (int i = 0; i < executed.length; i++) {
            assertThat(executed[i].get(), is(false));
        }
    }

    @Test
    public void testSingleCommand_ToolHelp() throws Exception {
        final AtomicReference<Boolean> helpWritten = new AtomicReference<>(false);
        Terminal terminal = new TerminalMock() {
            @Override
            public void println(String msg, Object... args) {
                assertThat(msg, equalTo("cmd1 help"));
                helpWritten.set(true);
            }
        };
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new IOException("io error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        int status = tool.execute(args("-h"));
        assertThat(status, is(CliTool.ExitStatus.OK.status()));
        assertThat(helpWritten.get(), is(true));
    }

    public void testMultiCommand_ToolHelp() {
        final AtomicReference<Boolean> helpWritten = new AtomicReference<>(false);
        Terminal terminal = new TerminalMock() {
            @Override
            public void println(String msg, Object... args) {
                assertThat(msg, equalTo("tool help"));
                helpWritten.set(true);
            }
        };
        NamedCommand[] cmds = new NamedCommand[2];
        cmds[0] = new NamedCommand("cmd0", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return CliTool.ExitStatus.OK;
            }
        };
        cmds[1] = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return CliTool.ExitStatus.OK;
            }
        };
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int status = tool.execute(args("-h"));
        assertThat(status, is(CliTool.ExitStatus.OK.status()));
        assertThat(helpWritten.get(), is(true));
    }

    public void testMultiCommand_CmdHelp() {
        final AtomicReference<Boolean> helpWritten = new AtomicReference<>(false);
        Terminal terminal = new TerminalMock() {
            @Override
            public void println(String msg, Object... args) {
                assertThat(msg, equalTo("cmd1 help"));
                helpWritten.set(true);
            }
        };
        NamedCommand[] cmds = new NamedCommand[2];
        cmds[0] = new NamedCommand("cmd0", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return CliTool.ExitStatus.OK;
            }
        };
        cmds[1] = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return CliTool.ExitStatus.OK;
            }
        };
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int status = tool.execute(args("cmd1 -h"));
        assertThat(status, is(CliTool.ExitStatus.OK.status()));
        assertThat(helpWritten.get(), is(true));
    }

    private static class SingleCmdTool extends CliTool {

        private final Command command;

        private SingleCmdTool(String name, Terminal terminal, NamedCommand command) {
            super(CliToolConfig.config(name, SingleCmdTool.class)
                    .cmds(cmd(command.name, command.getClass()))
                    .build(), terminal);
            this.command = command;
        }

        @Override
        protected Command parse(String cmdName, CommandLine cli) throws Exception {
            return command;
        }
    }

    private static class MultiCmdTool extends CliTool {

        private final Map<String, Command> commands;

        private MultiCmdTool(String name, Terminal terminal, NamedCommand... commands) {
            super(CliToolConfig.config(name, MultiCmdTool.class)
                    .cmds(cmds(commands))
                    .build(), terminal);
            ImmutableMap.Builder<String, Command> commandByName = ImmutableMap.builder();
            for (int i = 0; i < commands.length; i++) {
                commandByName.put(commands[i].name, commands[i]);
            }
            this.commands = commandByName.build();
        }

        @Override
        protected Command parse(String cmdName, CommandLine cli) throws Exception {
            return commands.get(cmdName);
        }

        private static CliToolConfig.Cmd[] cmds(NamedCommand... commands) {
            CliToolConfig.Cmd[] cmds = new CliToolConfig.Cmd[commands.length];
            for (int i = 0; i < commands.length; i++) {
                cmds[i] = cmd(commands[i].name, commands[i].getClass()).build();
            }
            return cmds;
        }
    }

    private static abstract class NamedCommand extends CliTool.Command {

        private final String name;

        private NamedCommand(String name, Terminal terminal) {
            super(terminal);
            this.name = name;
        }
    }


}
