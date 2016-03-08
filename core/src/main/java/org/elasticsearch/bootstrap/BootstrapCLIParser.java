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

import java.util.Arrays;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.Strings;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.monitor.jvm.JvmInfo;

final class BootstrapCliParser extends Command {

    private final OptionSpec<Void> versionOption;
    private final OptionSpec<Void> daemonizeOption;
    private final OptionSpec<String> pidfileOption;
    private final OptionSpec<String> propertyOption;
    private boolean shouldRun = false;

    BootstrapCliParser() {
        super("Starts elasticsearch");
        // TODO: in jopt-simple 5.0, make this mutually exclusive with all other options
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"),
            "Prints elasticsearch version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"),
            "Starts Elasticsearch in the background");
        // TODO: in jopt-simple 5.0 this option type can be a Path
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"),
            "Creates a pid file in the specified path on start")
            .withRequiredArg();
        propertyOption = parser.accepts("E", "Configures an Elasticsearch setting")
            .withRequiredArg();
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        if (options.has(versionOption)) {
            terminal.println("Version: " + org.elasticsearch.Version.CURRENT
                + ", Build: " + Build.CURRENT.shortHash() + "/" + Build.CURRENT.date()
                + ", JVM: " + JvmInfo.jvmInfo().version());
            return;
        }

        // TODO: don't use sysprops for any of these! pass the args through to bootstrap...
        if (options.has(daemonizeOption)) {
            System.setProperty("es.foreground", "false");
        }
        String pidFile = pidfileOption.value(options);
        if (Strings.isNullOrEmpty(pidFile) == false) {
            System.setProperty("es.pidfile", pidFile);
        }

        for (String property : propertyOption.values(options)) {
            String[] keyValue = property.split("=", 2);
            if (keyValue.length != 2) {
                throw new UserError(ExitCodes.USAGE, "Malformed elasticsearch setting, must be of the form key=value");
            }
            System.setProperty("es." + keyValue[0], keyValue[1]);
        }
        shouldRun = true;
    }

    boolean shouldRun() {
        return shouldRun;
    }
}
