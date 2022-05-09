/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

/**
 * Starts an Elasticsearch server process, but does not wait for it.
 *
 * Closing this cli will stop the server process.
 */
class WindowsServiceServer extends EnvironmentAwareCommand {

    private volatile ServerProcess server;

    WindowsServiceServer() {
        super("Starts and stops the Elasticsearch server process for a Windows Service");
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        var args = new ServerArgs(false, true, null, new SecureString(""), env.settings(), env.configFile());
        this.server = ServerProcess.start(terminal, processInfo, args, env.pluginsFile());
        // start does not return until the server is ready, and we do not wait for the process
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop();
        }
    }
}
