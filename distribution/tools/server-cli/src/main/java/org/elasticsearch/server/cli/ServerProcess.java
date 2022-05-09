/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.io.IOException;
import java.nio.file.Path;

interface ServerProcess {

    void detach() throws IOException;

    void waitFor() throws UserException, InterruptedException;

    void stop();

    static ServerProcess start(Terminal terminal, ProcessInfo processInfo, ServerArgs args, Path pluginsDir) throws UserException {
        return new JavaServerProcess(terminal, processInfo, args, pluginsDir);
    }
}
