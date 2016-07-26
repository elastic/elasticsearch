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

package org.elasticsearch.index.translog;

import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.translog.TruncateTranslogCommand;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

/**
 * Class encapsulating and dispatching commands from the {@code elasticsearch-translog} command line tool
 */
public class TranslogToolCli extends MultiCommand {

    public TranslogToolCli() {
        super("A CLI tool for various Elasticsearch translog actions");
        subcommands.put("truncate", new TruncateTranslogCommand());
    }

    public static void main(String[] args) throws Exception {
        // initialize default for es.logger.level because we will not read the logging.yml
        String loggerLevel = System.getProperty("es.logger.level", "INFO");
        String pathHome = System.getProperty("es.path.home");
        // Set the appender for all potential log files to terminal so that other components that use the logger print out the
        // same terminal.
        Environment loggingEnvironment = InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put("path.home", pathHome)
                .put("appender.terminal.type", "terminal")
                .put("rootLogger", "${logger.level}, terminal")
                .put("logger.level", loggerLevel)
                .build(), Terminal.DEFAULT);
        LogConfigurator.configure(loggingEnvironment.settings(), false);

        exit(new TranslogToolCli().main(args, Terminal.DEFAULT));
    }
}
