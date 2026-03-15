/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A test command that prints user output and optionally writes sentinel bytes
 * to the terminal's output stream. Used to verify redirect behavior.
 */
public class RedirectTestCommand extends Command {

    static final String USER_OUTPUT = "user-output";
    static final byte[] SENTINEL_BYTES = "DESC".getBytes(StandardCharsets.UTF_8);

    RedirectTestCommand() {
        super("redirect test command");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        terminal.println(USER_OUTPUT);
        var out = terminal.getOutputStream();
        if (out != null) {
            out.write(SENTINEL_BYTES);
            out.flush();
        }
    }

    @Override
    public void close() throws IOException {}
}
