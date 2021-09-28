/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.plugins.PluginLogger;

public class TerminalLogger implements PluginLogger {

    private final Terminal delegate;

    public TerminalLogger(Terminal delegate) {
        this.delegate = delegate;
    }

    @Override
    public void debug(String message) {
        this.delegate.println(Terminal.Verbosity.VERBOSE, message);
    }

    @Override
    public void info(String message) {
        this.delegate.println(Terminal.Verbosity.NORMAL, message);
    }

    @Override
    public void warn(String message) {
        this.delegate.errorPrintln(Terminal.Verbosity.NORMAL, message);
    }

    @Override
    public void error(String message) {
        this.delegate.errorPrintln(Terminal.Verbosity.SILENT, message);
    }

    @Override
    public String readText(String prompt) {
        return delegate.readText(prompt);
    }
}
