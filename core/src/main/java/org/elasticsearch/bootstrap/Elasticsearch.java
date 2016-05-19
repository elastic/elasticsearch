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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.Build;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.SettingCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * This class starts elasticsearch.
 */
class Elasticsearch extends SettingCommand {

    private final OptionSpec<Void> versionOption;
    private final OptionSpec<Void> daemonizeOption;
    private final OptionSpec<String> pidfileOption;

    // visible for testing
    Elasticsearch() {
        super("starts elasticsearch");
        // TODO: in jopt-simple 5.0, make this mutually exclusive with all other options
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"),
            "Prints elasticsearch version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"),
            "Starts Elasticsearch in the background");
        // TODO: in jopt-simple 5.0 this option type can be a Path
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"),
            "Creates a pid file in the specified path on start")
            .withRequiredArg();
    }

    /**
     * Main entry point for starting elasticsearch
     */
    public static void main(final String[] args) throws Exception {
        final Elasticsearch elasticsearch = new Elasticsearch();
        int status = main(args, elasticsearch, Terminal.DEFAULT);
        if (status != ExitCodes.OK) {
            exit(status);
        }
    }

    static int main(final String[] args, final Elasticsearch elasticsearch, final Terminal terminal) throws Exception {
        return elasticsearch.main(args, terminal);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Map<String, String> settings) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserError(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            if (options.has(daemonizeOption) || options.has(pidfileOption)) {
                throw new UserError(ExitCodes.USAGE, "Elasticsearch version option is mutually exclusive with any other option");
            }
            terminal.println("Version: " + org.elasticsearch.Version.CURRENT
                    + ", Build: " + Build.CURRENT.shortHash() + "/" + Build.CURRENT.date()
                    + ", JVM: " + JvmInfo.jvmInfo().version());
            return;
        }

        final boolean daemonize = options.has(daemonizeOption);
        final String pidFile = pidfileOption.value(options);

        init(daemonize, pidFile, settings);
    }

    void init(final boolean daemonize, final String pidFile, final Map<String, String> esSettings) {
        try {
            Bootstrap.init(!daemonize, pidFile, esSettings);
        } catch (final Throwable t) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupError(t);
        }
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to update service.bat!
     */
    static void close(String[] args) throws IOException {
        Bootstrap.stop();
    }
}
