/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.server.launcher.common.ProcessUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * A thread that reads the preparer process's stderr and dispatches each line to
 * either stdout or stderr of the launcher based on a tag prefix.
 *
 * <p> Lines starting with {@link #STDERR_LINE_TAG} ({@code \u0001}) are stderr-destined:
 * the tag is stripped and the line is written to the launcher's stderr. All other lines
 * are written to the launcher's stdout. This matches the tagging applied by
 * {@code StderrTaggingOutputStream} in the cli-launcher module.
 */
class PreparerOutputPump extends Thread {

    /**
     * Tag character prepended to stderr-destined lines by the preparer process.
     * Must match {@code StderrTaggingOutputStream.STDERR_LINE_TAG} in the cli-launcher module.
     */
    static final char STDERR_LINE_TAG = '\u0001';

    private final BufferedReader reader;
    private final PrintStream stdout;
    private final PrintStream stderr;

    PreparerOutputPump(InputStream input, PrintStream stdout, PrintStream stderr) {
        super("server-launcher[preparer_output]");
        this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        this.stdout = stdout;
        this.stderr = stderr;
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() == false && line.charAt(0) == STDERR_LINE_TAG) {
                    stderr.println(line.substring(1));
                } else {
                    stdout.println(line);
                }
            }
        } catch (IOException e) {
            // stream closed, nothing to do
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Waits for the pump thread to finish reading all output.
     */
    void drain() {
        ProcessUtil.nonInterruptibleVoid(this::join);
    }
}
