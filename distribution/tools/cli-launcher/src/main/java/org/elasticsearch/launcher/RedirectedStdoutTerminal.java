/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.SuppressForbidden;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

/**
 * A terminal used when the process is run with ES_REDIRECT_STDOUT_TO_STDERR
 * (or cli.redirectStdoutToStderr). At construction time the calling code has already
 * installed an {@link OutputStreamMux} so that both {@code System.out} and
 * {@code System.err} write to the same underlying pipe with byte-level mode markers.
 *
 * <p> This terminal wires {@code outWriter} to {@code System.out} (stdout channel) and
 * {@code errWriter} to {@code System.err} (stderr channel). Binary data (e.g. the launch
 * descriptor) is written via {@link #getOutputStream()} which returns the saved original stdout.
 */
class RedirectedStdoutTerminal extends Terminal {

    private final OutputStream stdoutForBinary;

    @SuppressForbidden(reason = "System.out and System.err are mux channels installed by CliToolLauncher")
    RedirectedStdoutTerminal(OutputStream stdoutForBinary) {
        super(
            new InputStreamReader(System.in, Charset.defaultCharset()),
            new PrintWriter(System.out, true),
            new PrintWriter(System.err, true)
        );
        this.stdoutForBinary = stdoutForBinary;
    }

    @Override
    @SuppressForbidden(reason = "Expose stdin for binary input (e.g. keystore prompts)")
    public InputStream getInputStream() {
        return System.in;
    }

    @Override
    public OutputStream getOutputStream() {
        return stdoutForBinary;
    }
}
