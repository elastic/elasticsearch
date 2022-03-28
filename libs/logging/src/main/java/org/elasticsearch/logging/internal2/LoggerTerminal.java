/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal2;

import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.internal.Util;

import java.io.OutputStream;
import java.io.PrintWriter;

public final class LoggerTerminal extends Terminal {
    private final ExtendedLoggerWrapper logger;

    private static final String FQCN = LoggerTerminal.class.getName();

    private LoggerTerminal(final Logger logger) {
        super(System.lineSeparator());
        this.logger = new ExtendedLoggerWrapper(
            (AbstractLogger) logger,
            Util.log4jLogger(logger).getName(),
            Util.log4jLogger(logger).getMessageFactory()
        );
    }

    public static LoggerTerminal getLogger(String logger) {
        return new LoggerTerminal(LogManager.getLogger(logger));
    }

    @Override
    public boolean isHeadless() {
        return true;
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
    public char[] readSecret(String prompt, int maxLength) {
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

    @Override
    public PrintWriter getErrorWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void print(Verbosity verbosity, String msg, boolean isError) {
        Level level = switch (verbosity) {
            case SILENT -> isError ? Level.ERROR : Level.WARN;
            case VERBOSE -> Level.DEBUG;
            case NORMAL -> isError ? Level.WARN : Level.INFO;
        };
        this.logger.logIfEnabled(FQCN, Util.log4jLevel(level), null, msg.trim(), (Throwable) null);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }
}
