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

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.cli.CliTool.ExitStatus.OK;
import static org.elasticsearch.common.cli.CliTool.ExitStatus.USAGE;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "modifies system properties intentionally")
public class CliToolTests extends CliToolTestCase {
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

    public void testUsageError() throws Exception {
        Terminal terminal = new MockTerminal();
        final AtomicReference<Boolean> executed = new AtomicReference<>(false);
        final NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws UserError {
                executed.set(true);
                throw new UserError(CliTool.ExitStatus.USAGE, "bad usage");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        CliTool.ExitStatus status = tool.execute();
        assertStatus(status, CliTool.ExitStatus.USAGE);
        assertCommandHasBeenExecuted(executed);
    }

    public void testMultiCommand() throws Exception {
        Terminal terminal = new MockTerminal();
        int count = randomIntBetween(2, 7);
        List<AtomicReference<Boolean>> executed = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            executed.add(new AtomicReference<>(false));
        }
        NamedCommand[] cmds = new NamedCommand[count];
        for (int i = 0; i < count; i++) {
            final int index = i;
            cmds[i] = new NamedCommand("cmd" + index, terminal) {
                @Override
                public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                    executed.get(index).set(true);
                    return OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        int cmdIndex = randomIntBetween(0, count-1);
        CliTool.ExitStatus status = tool.execute("cmd" + cmdIndex);
        assertThat(status, is(OK));
        for (int i = 0; i < count; i++) {
            assertThat(executed.get(i).get(), is(i == cmdIndex));
        }
    }

    public void testMultiCommandUnknownCommand() throws Exception {
        Terminal terminal = new MockTerminal();
        int count = randomIntBetween(2, 7);
        List<AtomicReference<Boolean>> executed = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            executed.add(new AtomicReference<>(false));
        }
        NamedCommand[] cmds = new NamedCommand[count];
        for (int i = 0; i < count; i++) {
            final int index = i;
            cmds[i] = new NamedCommand("cmd" + index, terminal) {
                @Override
                public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                    executed.get(index).set(true);
                    return OK;
                }
            };
        }
        MultiCmdTool tool = new MultiCmdTool("tool", terminal, cmds);
        CliTool.ExitStatus status = tool.execute("cmd" + count); // "cmd" + count doesn't exist
        assertThat(status, is(CliTool.ExitStatus.USAGE));
        for (int i = 0; i < count; i++) {
            assertThat(executed.get(i).get(), is(false));
        }
    }

    public void testSingleCommandToolHelp() throws Exception {
        MockTerminal terminal = new MockTerminal();
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
        assertThat(terminal.getOutput(), containsString("cmd1 help"));
    }

    public void testMultiCommandToolHelp() throws Exception {
        MockTerminal terminal = new MockTerminal();
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
        assertThat(terminal.getOutput(), containsString("tool help"));
    }

    public void testMultiCommandCmdHelp() throws Exception {
        MockTerminal terminal = new MockTerminal();
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
        assertThat(terminal.getOutput(), containsString("cmd1 help"));
    }

    public void testNonUserErrorPropagates() throws Exception {
        MockTerminal terminal = new MockTerminal();
        NamedCommand cmd = new NamedCommand("cmd", terminal) {
            @Override
            public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
                throw new IOException("error message");
            }
        };
        SingleCmdTool tool = new SingleCmdTool("tool", terminal, cmd);
        IOException e = expectThrows(IOException.class, () -> {
           tool.execute();
        });
        assertEquals("error message", e.getMessage());
    }

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

    public void testPromptForSetting() throws Exception {
        final AtomicReference<String> promptedSecretValue = new AtomicReference<>(null);
        final AtomicReference<String> promptedTextValue = new AtomicReference<>(null);
        final MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("replaced");
        terminal.addSecretInput("changeit");
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

        assertThat(promptedSecretValue.get(), is("changeit"));
        assertThat(promptedTextValue.get(), is("replaced"));
    }

    public void testStopAtNonOptionParsing() throws Exception {
        final CliToolConfig.Cmd lenientCommand = cmd("lenient", CliTool.Command.Exit.class).stopAtNonOption(true).build();
        final CliToolConfig.Cmd strictCommand = cmd("strict", CliTool.Command.Exit.class).stopAtNonOption(false).build();
        final CliToolConfig config = CliToolConfig.config("elasticsearch", CliTool.class).cmds(lenientCommand, strictCommand).build();

        MockTerminal terminal = new MockTerminal();
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
        assertThat(terminal.getOutput(), containsString("Unrecognized option: --unknown"));

        terminal.resetOutput();
        assertStatus(cliTool.execute(args("strict -u")), USAGE);
        assertThat(terminal.getOutput(), containsString("Unrecognized option: -u"));
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
            Map<String, Command> commandByName = new HashMap<>();
            for (int i = 0; i < commands.length; i++) {
                commandByName.put(commands[i].name, commands[i]);
            }
            this.commands = unmodifiableMap(commandByName);
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
