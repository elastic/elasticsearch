/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.elasticsearch.logging.Level;

public final class LoggerImpl implements org.elasticsearch.logging.Logger {

    private final org.apache.logging.log4j.Logger log4jLogger;

    LoggerImpl(org.apache.logging.log4j.Logger log4jLogger) {
        this.log4jLogger = log4jLogger;
    }

    @Override
    public Level getLevel() {
        return Util.elasticsearchLevel(log4jLogger.getLevel());
    }

    @Override
    public String getName() {
        return log4jLogger.getName();
    }

    @Override
    public boolean isLoggable(org.elasticsearch.logging.Level level) {
        return log4jLogger.isEnabled(log4jLevel(level));
    }

    @Override
    public void log(Level level, Object message, Object... params) {

    }

    @Override
    public void log(org.elasticsearch.logging.Level level, String message, Object... params) {
        log4jLogger.log(log4jLevel(level), message, params);
    }

    @Override
    public void log(org.elasticsearch.logging.Level level, String message, Throwable throwable) {
        log4jLogger.log(log4jLevel(level), message, throwable);
    }

    // @Override
    // public void log(String message, java.util.function.Supplier<?>... paramSuppliers) {
    // log4jLogger.log(log4jLevel(level), message, throwable);
    // }

    /*package*/ org.apache.logging.log4j.Logger log4jLogger() {
        return log4jLogger;
    }

    private static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return Util.log4jLevel(level);
    }
}
