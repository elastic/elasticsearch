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

package org.elasticsearch.common.cli;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.cli.CliTool.ExitStatus.OK;
import static org.elasticsearch.common.cli.CliTool.ExitStatus.USAGE;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CliToolTests extends CliToolTestCase {

    @Test
    public void testOK() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                executed.set(true);
                return OK;
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute();
        assertStatus(status, OK);
        assertCommandHasBeenExecuted(executed);
    }

    @Test
    public void testUsageError() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                executed.set(true);
                return CliTool.ExitStatus.USAGE;
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute();
        assertStatus(status, CliTool.ExitStatus.USAGE);
        assertCommandHasBeenExecuted(executed);
    }

    @Test
    public void testIOError() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new IOException("io error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute();
        assertStatus(status, CliTool.ExitStatus.IO_ERROR);
        assertCommandHasBeenExecuted(executed);
    }

    @Test
    public void testCodeError() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new Exception("random error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute();
        assertStatus(status, CliTool.ExitStatus.CODE_ERROR);
        assertCommandHasBeenExecuted(executed);
    }

    @Test
    public void testMultiCommand() {
        Terminal terminal = new MockTerminal();
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
                    return OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int cmdIndex = randomIntBetween(0, count-1);
        CliTool.ExitStatus status = tool.execute("cmd" + cmdIndex);
        assertThat(status, is(OK));
        for (int i = 0; i < executed.length; i++) {
            assertThat(executed[i].get(), is(i == cmdIndex));
        }
    }

    @Test
    public void testMultiCommand_UnknownCommand() {
        Terminal terminal = new MockTerminal();
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
                    return OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        CliTool.ExitStatus status = tool.execute("cmd" + count); // "cmd" + count doesn't exist
        assertThat(status, is(CliTool.ExitStatus.USAGE));
        for (int i = 0; i < executed.length; i++) {
            assertThat(executed[i].get(), is(false));
        }
    }

    @Test
    public void testSingleCommand_ToolHelp() throws Exception {
        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                executed.set(true);
                throw new IOException("io error");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute(args("-h"));
        assertStatus(status, CliTool.ExitStatus.OK_AND_EXIT);
        assertThat(terminal.getTerminalOutput(), hasSize(3));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("cmd1 help")));
    }

    @Test
    public void testMultiCommand_ToolHelp() {
        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        NamedCommand[] cmds = new NamedCommand[2];
        cmds[0] = new NamedCommand("cmd0", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return OK;
            }
        };
        cmds[1] = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return OK;
            }
        };
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        CliTool.ExitStatus status = tool.execute(args("-h"));
        assertStatus(status, CliTool.ExitStatus.OK_AND_EXIT);
        assertThat(terminal.getTerminalOutput(), hasSize(3));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("tool help")));
    }

    @Test
    public void testMultiCommand_CmdHelp() {
        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        NamedCommand[] cmds = new NamedCommand[2];
        cmds[0] = new NamedCommand("cmd0", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return OK;
            }
        };
        cmds[1] = new NamedCommand("cmd1", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                return OK;
            }
        };
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        CliTool.ExitStatus status = tool.execute(args("cmd1 -h"));
        assertStatus(status, CliTool.ExitStatus.OK_AND_EXIT);
        assertThat(terminal.getTerminalOutput(), hasSize(3));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("cmd1 help")));
    }

    @Test
    public void testThatThrowExceptionCanBeLogged() throws Exception {
        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                throw new ElasticsearchException("error message");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        assertStatus(tool.execute(), CliTool.ExitStatus.CODE_ERROR);
        assertThat(terminal.getTerminalOutput(), hasSize(1));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("error message")));

        // set env... and log stack trace
        try {
            System.setProperty(Terminal.DEBUG_SYSTEM_PROPERTY, "true");
            terminal = new CaptureOutputTerminal();
            assertStatus(new SingleCmdTool("tool", terminal, cmd).execute(), CliTool.ExitStatus.CODE_ERROR);
            assertThat(terminal.getTerminalOutput(), hasSize(2));
            assertThat(terminal.getTerminalOutput(), hasItem(containsString("error message")));
            // This class must be part of the stack strace
            assertThat(terminal.getTerminalOutput(), hasItem(containsString(getClass().getName())));
        } finally {
            System.clearProperty(Terminal.DEBUG_SYSTEM_PROPERTY);
        }
    }

    @Test
    public void testMultipleLaunch() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                executed.set(true);
                return OK;
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        tool.parse("cmd", Strings.splitStringByCommaToArray("--verbose"));
        tool.parse("cmd", Strings.splitStringByCommaToArray("--silent"));
        tool.parse("cmd", Strings.splitStringByCommaToArray("--help"));
    }

    @Test
    public void testPromptForSetting() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicReference<String> promptedSecretValue = new AtomicReference<>(null);
        final AtomicReference<String> promptedTextValue = new AtomicReference<>(null);
        final Terminal terminal = new MockTerminal() {
            @Override
            public char[] readSecret(String text, Object... args) {
                counter.incrementAndGet();
                assertThat(args, arrayContaining((Object) "foo.password"));
                return "changeit".toCharArray();
            }

            @Override
            public String readText(String text, Object... args) {
                counter.incrementAndGet();
                assertThat(args, arrayContaining((Object) "replace"));
                return "replaced";
            }
        };
        final NamedCommand cmd = new NamedCommand("noop", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) {
                promptedSecretValue.set(settings.get("foo.password"));
                promptedTextValue.set(settings.get("replace"));
                return OK;
            }
        };

        System.setProperty("es.foo.password", InternalSettingsPreparer.SECRET_PROMPT_VALUE);
        System.setProperty("es.replace", InternalSettingsPreparer.TEXT_PROMPT_VALUE);
        try {
            new SingleCmdTool("tool", terminal, cmd).execute();
        } finally {
            System.clearProperty("es.foo.password");
            System.clearProperty("es.replace");
        }

        assertThat(counter.intValue(), is(2));
        assertThat(promptedSecretValue.get(), is("changeit"));
        assertThat(promptedTextValue.get(), is("replaced"));
    }

    @Test
    public void testStopAtNonOptionParsing() throws Exception {
        final CliToolConfig.Cmd lenientCommand = cmd("lenient", CliTool.Command.Exit.class).stopAtNonOption(true).build();
        final CliToolConfig.Cmd strictCommand = cmd("strict", CliTool.Command.Exit.class).stopAtNonOption(false).build();
        final CliToolConfig config = CliToolConfig.config("elasticsearch", CliTool.class).cmds(lenientCommand, strictCommand).build();

        final CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        final CliTool cliTool = new CliTool(config, terminal) {
            @Override
            protected Command parse(String cmdName, CommandLine cli) throws Exception {
                return new NamedCommand(cmdName, terminal) {
                    @Override
                    public ExitStatus execute(Settings settings, Environment env) throws Exception {
                        return OK;
                    }
                };
            }
        };

        // known parameters, no error
        assertStatus(cliTool.execute(args("lenient --verbose")), OK);
        assertStatus(cliTool.execute(args("lenient -v")), OK);

        // unknown parameters, no error
        assertStatus(cliTool.execute(args("lenient --unknown")), OK);
        assertStatus(cliTool.execute(args("lenient -u")), OK);

        // unknown parameters, error
        assertStatus(cliTool.execute(args("strict --unknown")), USAGE);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Unrecognized option: --unknown")));

        terminal.getTerminalOutput().clear();
        assertStatus(cliTool.execute(args("strict -u")), USAGE);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Unrecognized option: -u")));
    }

    private void assertStatus(CliTool.ExitStatus status, CliTool.ExitStatus expectedStatus) {
        assertThat(status, is(expectedStatus));
    }

    private void assertCommandHasBeenExecuted(AtomicReference<Boolean> executed) {
        assertThat("Expected command atomic reference counter to be set to true", executed.get(), is(Boolean.TRUE));
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
