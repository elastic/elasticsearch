/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;

import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

/**
 * Prints the JVM options that run a command line tool in FIPS 140 mode, one per line, or nothing when the node is not in FIPS mode.
 *
 * <p> The command line tools cannot read {@code elasticsearch.yml} before their JVM starts, and the shell scripts should not parse
 * it. So {@code bin/elasticsearch-cli} does a cheap scan of the file, the tool's {@code -E} arguments and the environment for the
 * FIPS mode setting, and only on a hit runs this command, which resolves the settings the same way the tool itself will. The
 * script passes the printed options to the tool's JVM and, when there are any, adds {@code lib/fips} to its class path.
 * {@code bin/elasticsearch} does the same for the server CLI, through {@code CLI_JAVA_OPTS}.
 *
 * <p> The paths are printed relative to ES_HOME, which is the working directory of every JVM the scripts start, so that the
 * options never contain a space and survive the whitespace split of {@code CLI_JAVA_OPTS} in the server launcher.
 */
class FipsOptionsCommand extends EnvironmentAwareCommand {

    FipsOptionsCommand() {
        super("Prints the JVM options for FIPS 140 mode");
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        Path relativeLib = FipsOptions.fipsLib(Path.of(""));
        for (String property : FipsOptions.systemProperties(env.settings(), processInfo.workingDir(), relativeLib)) {
            terminal.println(property);
        }
    }
}
