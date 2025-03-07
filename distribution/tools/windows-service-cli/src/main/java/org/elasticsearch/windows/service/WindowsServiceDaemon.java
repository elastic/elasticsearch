/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import joptsimple.OptionSet;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.server.cli.JvmOptionsParser;
import org.elasticsearch.server.cli.MachineDependentHeap;
import org.elasticsearch.server.cli.ServerProcess;
import org.elasticsearch.server.cli.ServerProcessBuilder;
import org.elasticsearch.server.cli.ServerProcessUtils;

import java.io.IOException;

/**
 * Starts an Elasticsearch process, but does not wait for it to exit.
 * <p>
 * This class is expected to be run via Apache Procrun in a long-lived JVM that will call close
 * when the server should shut down.
 */
class WindowsServiceDaemon extends EnvironmentAwareCommand {

    private volatile ServerProcess server;

    WindowsServiceDaemon() {
        super("Starts and stops the Elasticsearch server process for a Windows Service");
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        // the Windows service daemon doesn't support secure settings implementations other than the keystore
        try (var loadedSecrets = KeyStoreWrapper.bootstrap(env.configDir(), () -> new SecureString(new char[0]))) {
            var args = new ServerArgs(false, true, null, loadedSecrets, env.settings(), env.configDir(), env.logsDir());
            var tempDir = ServerProcessUtils.setupTempDir(processInfo);
            var jvmOptions = JvmOptionsParser.determineJvmOptions(args, processInfo, tempDir, new MachineDependentHeap());
            var serverProcessBuilder = new ServerProcessBuilder().withTerminal(terminal)
                .withProcessInfo(processInfo)
                .withServerArgs(args)
                .withTempDir(tempDir)
                .withJvmOptions(jvmOptions);
            this.server = serverProcessBuilder.start();
            // start does not return until the server is ready, and we do not wait for the process
        }
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.stop();
        }
    }
}
