/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.elasticsearch.cli.Terminal;

import java.io.OutputStream;
import java.io.PrintWriter;

public final class LoggerTerminal extends Terminal {
    private final ExtendedLoggerWrapper logger;

    private static final String FQCN = LoggerTerminal.class.getName();

    private LoggerTerminal(final Logger logger) {
        super(System.lineSeparator());
        this.logger = new ExtendedLoggerWrapper((AbstractLogger) logger, logger.getName(), logger.getMessageFactory());
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
        this.logger.logIfEnabled(FQCN, level, null, msg.trim(), (Throwable) null);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }
}
