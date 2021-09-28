/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.elasticsearch.cli.Terminal;

import java.io.OutputStream;
import java.io.PrintWriter;

public class LoggingTerminal extends Terminal {

    public LoggingTerminal(String lineSeparator) {
        super(lineSeparator);
    }

    @Override
    public String readText(String prompt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char[] readSecret(String prompt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrintWriter getWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream getOutputStream() {
        throw new UnsupportedOperationException();
    }


}
