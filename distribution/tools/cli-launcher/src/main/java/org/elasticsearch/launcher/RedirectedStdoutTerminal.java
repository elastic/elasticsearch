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
 * (or cli.redirectStdoutToStderr). At construction time the calling code has already:
 * <ol>
 *     <li>Redirected {@code System.out} to the real stderr (untagged, stdout-destined)</li>
 *     <li>Replaced {@code System.err} with a {@link StderrTaggingOutputStream} wrapper
 *         (tagged, stderr-destined)</li>
 * </ol>
 * This terminal wires {@code outWriter} to {@code System.out} (untagged) and
 * {@code errWriter} to {@code System.err} (tagged), so the launcher can distinguish
 * the original destination of each line. Binary data (e.g. the launch descriptor)
 * is written via {@link #getOutputStream()} which returns the saved original stdout.
 */
class RedirectedStdoutTerminal extends Terminal {

    private final OutputStream stdoutForBinary;

    @SuppressForbidden(reason = "System.out is untagged stderr (stdout-destined); System.err is tagged (stderr-destined)")
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
