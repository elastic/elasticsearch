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

package org.elasticsearch.plugins;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.util.Locale;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;

/**
 * A cli tool for adding, removing and listing plugins for elasticsearch.
 */
public class PluginCli extends CliTool {

    // commands
    private static final String LIST_CMD_NAME = "list";
    private static final String INSTALL_CMD_NAME = "install";
    private static final String REMOVE_CMD_NAME = "remove";

    // usage config
    private static final CliToolConfig.Cmd LIST_CMD = cmd(LIST_CMD_NAME, ListPluginsCommand.class).build();
    private static final CliToolConfig.Cmd INSTALL_CMD = cmd(INSTALL_CMD_NAME, InstallPluginCommand.class)
        .options(option("b", "batch").required(false))
        .build();
    private static final CliToolConfig.Cmd REMOVE_CMD = cmd(REMOVE_CMD_NAME, RemovePluginCommand.class).build();

    static final CliToolConfig CONFIG = CliToolConfig.config("plugin", PluginCli.class)
            .cmds(LIST_CMD, INSTALL_CMD, REMOVE_CMD)
            .build();

    public static void main(String[] args) {
        // initialize default for es.logger.level because we will not read the logging.yml
        String loggerLevel = System.getProperty("es.logger.level", "INFO");
        // Set the appender for all potential log files to terminal so that other components that use the logger print out the
        // same terminal.
        // The reason for this is that the plugin cli cannot be configured with a file appender because when the plugin command is
        // executed there is no way of knowing where the logfiles should be placed. For example, if elasticsearch
        // is run as service then the logs should be at /var/log/elasticsearch but when started from the tar they should be at es.home/logs.
        // Therefore we print to Terminal.
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put("appender.terminal.type", "terminal")
                .put("rootLogger", "${es.logger.level}, terminal")
                .put("es.logger.level", loggerLevel)
                .build(), Terminal.DEFAULT);
        // configure but do not read the logging conf file
        LogConfigurator.configure(env.settings(), false);
        int status = new PluginCli(Terminal.DEFAULT).execute(args).status();
        exit(status);
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    private static void exit(int status) {
        System.exit(status);
    }

    PluginCli(Terminal terminal) {
        super(CONFIG, terminal);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case LIST_CMD_NAME:
                return new ListPluginsCommand(terminal);
            case INSTALL_CMD_NAME:
                return parseInstallPluginCommand(cli);
            case REMOVE_CMD_NAME:
                return parseRemovePluginCommand(cli);
            default:
                assert false : "can't get here as cmd name is validated before this method is called";
                return exitCmd(ExitStatus.USAGE);
        }
    }

    private Command parseInstallPluginCommand(CommandLine cli) {
        String[] args = cli.getArgs();
        if (args.length != 1) {
            return exitCmd(ExitStatus.USAGE, terminal, "Must supply a single plugin id argument");
        }

        boolean batch = System.console() == null;
        if (cli.hasOption("b")) {
            batch = true;
        }

        return new InstallPluginCommand(terminal, args[0], batch);
    }

    private Command parseRemovePluginCommand(CommandLine cli) {
        String[] args = cli.getArgs();
        if (args.length != 1) {
            return exitCmd(ExitStatus.USAGE, terminal, "Must supply a single plugin name argument");
        }

        return new RemovePluginCommand(terminal, args[0]);
    }
}
