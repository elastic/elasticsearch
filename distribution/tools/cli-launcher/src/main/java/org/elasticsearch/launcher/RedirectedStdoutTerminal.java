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
 * A terminal that sends all print output to stderr and exposes the real stdout
 * via {@link #getOutputStream()}. Used when the process is run with
 * ES_REDIRECT_STDOUT_TO_STDERR (or cli.redirectStdoutToStderr) so that
 * binary data (e.g. the launch descriptor) can be written to stdout while
 * user-visible output goes to stderr.
 */
class RedirectedStdoutTerminal extends Terminal {

    private final OutputStream stdoutForBinary;

    @SuppressForbidden(reason = "Use stderr for all print output; stdout reserved for binary (e.g. descriptor)")
    RedirectedStdoutTerminal(OutputStream stdoutForBinary) {
        super(
            new InputStreamReader(System.in, Charset.defaultCharset()),
            new PrintWriter(System.err, true),
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
