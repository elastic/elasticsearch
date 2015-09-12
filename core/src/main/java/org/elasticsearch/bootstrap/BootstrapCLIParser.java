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

package org.elasticsearch.bootstrap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.optionBuilder;

final class BootstrapCLIParser extends CliTool {

    private static final CliToolConfig CONFIG = CliToolConfig.config("elasticsearch", BootstrapCLIParser.class)
            .cmds(Start.CMD, Version.CMD)
            .build();

    public BootstrapCLIParser() {
        super(CONFIG);
    }

    public BootstrapCLIParser(Terminal terminal) {
        super(CONFIG, terminal);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case Start.NAME:
                return Start.parse(terminal, cli);
            case Version.NAME:
                return Version.parse(terminal, cli);
            default:
                assert false : "should never get here, if the user enters an unknown command, an error message should be shown before parse is called";
                return null;
        }
    }

    static class Version extends CliTool.Command {

        private static final String NAME = "version";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Version.class).build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            return new Version(terminal);
        }

        public Version(Terminal terminal) {
            super(terminal);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            terminal.println("Version: %s, Build: %s/%s, JVM: %s", org.elasticsearch.Version.CURRENT, Build.CURRENT.hashShort(), Build.CURRENT.timestamp(), JvmInfo.jvmInfo().version());
            return ExitStatus.OK_AND_EXIT;
        }
    }

    static class Start extends CliTool.Command {

        private static final String NAME = "start";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Start.class)
                .options(
                        optionBuilder("d", "daemonize").hasArg(false).required(false),
                        optionBuilder("p", "pidfile").hasArg(true).required(false),
                        optionBuilder("V", "version").hasArg(false).required(false),
                        Option.builder("D").argName("property=value").valueSeparator('=').numberOfArgs(2)
                )
                .stopAtNonOption(true) // needed to parse the --foo.bar options, so this parser must be lenient
                .build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            if (cli.hasOption("V")) {
                return Version.parse(terminal, cli);
            }

            if (cli.hasOption("d")) {
                System.setProperty("es.foreground", "false");
            }

            String pidFile = cli.getOptionValue("pidfile");
            if (!Strings.isNullOrEmpty(pidFile)) {
                System.setProperty("es.pidfile", pidFile);
            }

            if (cli.hasOption("D")) {
                Properties properties = cli.getOptionProperties("D");
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    String key = (String) entry.getKey();
                    String propertyName = key.startsWith("es.") ? key : "es." + key;
                    System.setProperty(propertyName, entry.getValue().toString());
                }
            }

            // hacky way to extract all the fancy extra args, there is no CLI tool helper for this
            Iterator<String> iterator = cli.getArgList().iterator();
            while (iterator.hasNext()) {
                String arg = iterator.next();
                if (!arg.startsWith("--")) {
                    throw new IllegalArgumentException("Parameter [" + arg + "]does not start with --");
                }
                // if there is no = sign, we have to get the next argu
                arg = arg.replace("--", "");
                if (arg.contains("=")) {
                    String[] splitArg = arg.split("=", 2);
                    String key = splitArg[0];
                    String value = splitArg[1];
                    System.setProperty("es." + key, value);
                } else {
                    if (iterator.hasNext()) {
                        String value = iterator.next();
                        if (value.startsWith("--")) {
                            throw new IllegalArgumentException("Parameter [" + arg + "] needs value");
                        }
                        System.setProperty("es." + arg, value);
                    } else {
                        throw new IllegalArgumentException("Parameter [" + arg + "] needs value");
                    }
                }
            }

            return new Start(terminal);
        }

        public Start(Terminal terminal) {
            super(terminal);

        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            return ExitStatus.OK;
        }
    }

}
