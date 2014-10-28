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

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * A base class for command-line interface tool.
 *
 * Two modes are supported:
 *
 * - Single command mode. The tool exposes a single command that can potentially accept arguments (eg. CLI options).
 * - Multi command mode. The tool support multiple commands, each for different tasks, each potentially accepts arguments.
 *
 * In a multi-command mode. The first argument must be the command name. For example, the plugin manager
 * can be seen as a multi-command tool with two possible commands: install and uninstall
 *
 * The tool is configured using a {@link CliToolConfig} which encapsulates the tool's commands and their
 * potential options. The tool also comes with out of the box simple help support (the -h/--help option is
 * automatically handled) where the help text is configured in a dedicated *.help files located in the same package
 * as the tool.
 */
public abstract class CliTool {

    // based on sysexits.h
    public static enum ExitStatus {
        OK(0),
        USAGE(64),          /* command line usage error */
        DATA_ERROR(65),     /* data format error */
        NO_INPUT(66),       /* cannot open input */
        NO_USER(67),        /* addressee unknown */
        NO_HOST(68),        /* host name unknown */
        UNAVAILABLE(69),    /* service unavailable */
        CODE_ERROR(70),     /* internal software error */
        CANT_CREATE(73),    /* can't create (user) output file */
        IO_ERROR(74),       /* input/output error */
        TEMP_FAILURE(75),   /* temp failure; user is invited to retry */
        PROTOCOL(76),       /* remote error in protocol */
        NOPERM(77),         /* permission denied */
        CONFIG(78);          /* configuration error */

        final int status;

        private ExitStatus(int status) {
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    protected final Terminal terminal;
    protected final Environment env;
    protected final Settings settings;

    private final CliToolConfig config;

    protected CliTool(CliToolConfig config) {
        this(config, Terminal.DEFAULT);
    }

    protected CliTool(CliToolConfig config, Terminal terminal) {
        Preconditions.checkArgument(config.cmds().size() != 0, "At least one command must be configured");
        this.config = config;
        this.terminal = terminal;
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true);
        settings = tuple.v1();
        env = tuple.v2();
    }

    public final int execute(String... args) {

        // first lets see if the user requests tool help. We're doing it only if
        // this is a multi-command tool. If it's a single command tool, the -h/--help
        // option will be taken care of on the command level
        if (!config.isSingle() && args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            config.printUsage(terminal);
            return ExitStatus.OK.status;
        }

        CliToolConfig.Cmd cmd;
        if (config.isSingle()) {
            cmd = config.single();
        } else {

            if (args.length == 0) {
                terminal.printError("command not specified");
                config.printUsage(terminal);
                return ExitStatus.USAGE.status;
            }

            String cmdName = args[0];
            cmd = config.cmd(cmdName);
            if (cmd == null) {
                terminal.printError("unknown command [%s]. Use [-h] option to list available commands", cmdName);
                return ExitStatus.USAGE.status;
            }

            // we now remove the command name from the args
            if (args.length == 1) {
                args = new String[0];
            } else {
                String[] cmdArgs = new String[args.length - 1];
                System.arraycopy(args, 1, cmdArgs, 0, cmdArgs.length);
                args = cmdArgs;
            }
        }

        Command command = null;
        try {

            command = parse(cmd, args);
            return command.execute(settings, env).status;

        } catch (IOException ioe) {
            terminal.printError(ioe);
            return ExitStatus.IO_ERROR.status;
        } catch (IllegalArgumentException | ElasticsearchIllegalArgumentException ilae) {
            terminal.printError(ilae);
            return ExitStatus.USAGE.status;
        } catch (Throwable t) {
            terminal.printError(t);
            if (command == null) {
                return ExitStatus.USAGE.status;
            }
            return ExitStatus.CODE_ERROR.status;
        }
    }

    public Command parse(String cmdName, String[] args) throws Exception {
        CliToolConfig.Cmd cmd = config.cmd(cmdName);
        return parse(cmd, args);
    }

    public Command parse(CliToolConfig.Cmd cmd, String[] args) throws Exception {
        CommandLineParser parser = new GnuParser();
        CommandLine cli = parser.parse(CliToolConfig.OptionsSource.HELP.options(), args, true);
        if (cli.hasOption("h")) {
            return helpCmd(cmd);
        }
        cli = parser.parse(cmd.options(), args);
        Terminal.Verbosity verbosity = Terminal.Verbosity.resolve(cli);
        terminal.verbosity(verbosity);
        return parse(cmd.name(), cli);
    }

    protected Command.Help helpCmd(CliToolConfig.Cmd cmd) {
        return new Command.Help(cmd, terminal);
    }

    protected static Command.Exit exitCmd(ExitStatus status) {
        return new Command.Exit(null, status, null);
    }

    protected static Command.Exit exitCmd(ExitStatus status, Terminal terminal, String msg, Object... args) {
        return new Command.Exit(String.format(Locale.ROOT, msg, args), status, terminal);
    }

    protected abstract Command parse(String cmdName, CommandLine cli) throws Exception;

    public static abstract class Command {

        protected final Terminal terminal;

        protected Command(Terminal terminal) {
            this.terminal = terminal;
        }

        public abstract ExitStatus execute(Settings settings, Environment env) throws Exception;

        public static class Help extends Command {

            private final CliToolConfig.Cmd cmd;

            private Help(CliToolConfig.Cmd cmd, Terminal terminal) {
                super(terminal);
                this.cmd = cmd;
            }

            @Override
            public ExitStatus execute(Settings settings, Environment env) throws Exception {
                cmd.printUsage(terminal);
                return ExitStatus.OK;
            }
        }

        public static class Exit extends Command {
            private final String msg;
            private final ExitStatus status;

            private Exit(String msg, ExitStatus status, Terminal terminal) {
                super(terminal);
                this.msg = msg;
                this.status = status;
            }

            @Override
            public ExitStatus execute(Settings settings, Environment env) throws Exception {
                if (msg != null) {
                    if (status != ExitStatus.OK) {
                        terminal.printError(msg);
                    } else {
                        terminal.println(msg);
                    }
                }
                return status;
            }

            public ExitStatus status() {
                return status;
            }
        }
    }



}

