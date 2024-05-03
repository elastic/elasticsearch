/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.internal;

import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

import java.util.function.Supplier;

@SuppressLoggerChecks(reason = "safely delegates to logger")
public final class LoggerImpl implements Logger {
    private final org.apache.logging.log4j.Logger log4jLogger;

    public LoggerImpl(org.apache.logging.log4j.Logger log4jLogger) {
        this.log4jLogger = log4jLogger;
    }

    private static org.apache.logging.log4j.util.Supplier<?> mapSupplier(Supplier<String> msgSupplier) {
        return () -> msgSupplier.get();
    }

    @Override
    public void log(Level level, String message) {
        log4jLogger.log(LevelUtil.log4jLevel(level), message);
    }

    @Override
    public void log(Level level, Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.log(LevelUtil.log4jLevel(level), mapSupplier(messageSupplier), throwable);
    }

    @Override
    public String getName() {
        return log4jLogger.getName();
    }

    @Override
    public boolean isFatalEnabled() {
        return log4jLogger.isFatalEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return log4jLogger.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return log4jLogger.isWarnEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return log4jLogger.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return log4jLogger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return log4jLogger.isTraceEnabled();
    }

    @Override
    public boolean isEnabled(Level level) {
        return log4jLogger.isEnabled(LevelUtil.log4jLevel(level));
    }

    @Override
    public void fatal(Supplier<String> messageSupplier) {
        log4jLogger.fatal(mapSupplier(messageSupplier));
    }

    @Override
    public void fatal(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.fatal(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void fatal(String message) {
        log4jLogger.fatal(message);
    }

    @Override
    public void fatal(String message, Throwable throwable) {
        log4jLogger.fatal(message, throwable);
    }

    @Override
    public void fatal(String message, Object... params) {
        log4jLogger.fatal(message, params);
    }

    @Override
    public void error(Supplier<String> messageSupplier) {
        log4jLogger.error(mapSupplier(messageSupplier));
    }

    @Override
    public void error(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.error(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void error(String message) {
        log4jLogger.error(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        log4jLogger.error(message, throwable);
    }

    @Override
    public void error(String message, Object... params) {
        log4jLogger.error(message, params);
    }

    @Override
    public void warn(Supplier<String> messageSupplier) {
        log4jLogger.warn(mapSupplier(messageSupplier));
    }

    @Override
    public void warn(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.warn(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void warn(String message) {
        log4jLogger.warn(message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        log4jLogger.warn(message, throwable);
    }

    @Override
    public void warn(String message, Object... params) {
        log4jLogger.warn(message, params);
    }

    @Override
    public void info(Supplier<String> messageSupplier) {
        log4jLogger.info(mapSupplier(messageSupplier));
    }

    @Override
    public void info(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.info(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void info(String message) {
        log4jLogger.info(message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        log4jLogger.info(message, throwable);
    }

    @Override
    public void info(String message, Object... params) {
        log4jLogger.info(message, params);
    }

    @Override
    public void debug(Supplier<String> messageSupplier) {
        log4jLogger.debug(mapSupplier(messageSupplier));
    }

    @Override
    public void debug(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.debug(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void debug(String message) {
        log4jLogger.debug(message);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        log4jLogger.debug(message, throwable);
    }

    @Override
    public void debug(String message, Object... params) {
        log4jLogger.debug(message, params);
    }

    @Override
    public void trace(Supplier<String> messageSupplier) {
        log4jLogger.trace(mapSupplier(messageSupplier));
    }

    @Override
    public void trace(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.trace(mapSupplier(messageSupplier), throwable);
    }

    @Override
    public void trace(String message) {
        log4jLogger.trace(message);
    }

    @Override
    public void trace(String message, Throwable throwable) {
        log4jLogger.trace(message, throwable);
    }

    @Override
    public void trace(String message, Object... params) {
        log4jLogger.trace(message, params);
    }
}
