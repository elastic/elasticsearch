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

import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.util.Collections;

/**
 * A cli tool for adding, removing and listing plugins for elasticsearch.
 */
public class PluginCli extends MultiCommand {

    public PluginCli() {
        super("A tool for managing installed elasticsearch plugins");
        subcommands.put("list", new ListPluginsCommand());
        subcommands.put("install", new InstallPluginCommand());
        subcommands.put("remove", new RemovePluginCommand());
    }

    public static void main(String[] args) throws Exception {
        // initialize default for es.logger.level because we will not read the logging.yml
        String loggerLevel = System.getProperty("es.logger.level", "INFO");
        String pathHome = System.getProperty("es.path.home");
        // Set the appender for all potential log files to terminal so that other components that use the logger print out the
        // same terminal.
        // The reason for this is that the plugin cli cannot be configured with a file appender because when the plugin command is
        // executed there is no way of knowing where the logfiles should be placed. For example, if elasticsearch
        // is run as service then the logs should be at /var/log/elasticsearch but when started from the tar they should be at es.home/logs.
        // Therefore we print to Terminal.
        Environment loggingEnvironment = InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put("path.home", pathHome)
                .put("appender.terminal.type", "terminal")
                .put("rootLogger", "${logger.level}, terminal")
                .put("logger.level", loggerLevel)
                .build(), Terminal.DEFAULT);
        LogConfigurator.configure(loggingEnvironment.settings(), false);

        exit(new PluginCli().main(args, Terminal.DEFAULT));
    }

}
