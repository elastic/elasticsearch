/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.api.impl;

import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

import java.util.function.Supplier;

@SuppressLoggerChecks(reason = "safely delegates to logger")
public final class LoggerImpl implements Logger {
    private final org.apache.logging.log4j.Logger log4jLogger;

    //
    public LoggerImpl(org.apache.logging.log4j.Logger log4jLogger) {
        this.log4jLogger = log4jLogger;
    }

    /*package*/ org.apache.logging.log4j.Logger log4jLogger() {
        return log4jLogger;
    }

    private org.apache.logging.log4j.util.Supplier<?> mapSupplier(Supplier<String> msgSupplier) {
        return (org.apache.logging.log4j.util.Supplier<Object>) () -> msgSupplier.get();
    }

    public void log(Level level, String message) {
        log4jLogger.log(Util.log4jLevel(level), message);
    }

    public void log(Level level, Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.log(Util.log4jLevel(level), mapSupplier(messageSupplier), throwable);
    }

    public Level getLevel() {
        return Util.elasticsearchLevel(log4jLogger.getLevel());
    }

    public String getName() {
        return log4jLogger.getName();
    }

    public boolean isFatalEnabled() {
        return log4jLogger.isFatalEnabled();
    }

    public boolean isErrorEnabled() {
        return log4jLogger.isErrorEnabled();
    }

    public boolean isWarnEnabled() {
        return log4jLogger.isWarnEnabled();
    }

    public boolean isInfoEnabled() {
        return log4jLogger.isInfoEnabled();
    }

    public boolean isDebugEnabled() {
        return log4jLogger.isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return log4jLogger.isTraceEnabled();
    }

    public boolean isEnabled(Level level) {
        return log4jLogger.isEnabled(Util.log4jLevel(level));
    }

    public void fatal(Supplier<String> messageSupplier) {
        log4jLogger.fatal(mapSupplier(messageSupplier));
    }

    public void fatal(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.fatal(mapSupplier(messageSupplier), throwable);
    }

    public void fatal(String message) {
        log4jLogger.fatal(message);
    }

    public void fatal(String message, Throwable throwable) {
        log4jLogger.fatal(message, throwable);
    }

    public void fatal(String message, Object p0) {
        log4jLogger.fatal(message, p0);
    }

    public void fatal(String message, Object p0, Object p1) {
        log4jLogger.fatal(message, p0, p1);
    }

    public void fatal(String message, Object p0, Object p1, Object p2) {
        log4jLogger.fatal(message, p0, p1, p2);
    }

    public void fatal(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.fatal(message, p0, p1, p2, p3);
    }

    public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.fatal(message, p0, p1, p2, p3, p4);
    }

    public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.fatal(message, p0, p1, p2, p3, p4, p5);
    }

    public void fatal(String message, Object... params) {
        log4jLogger.fatal(message, params);
    }

    public void error(Supplier<String> messageSupplier) {
        log4jLogger.error(mapSupplier(messageSupplier));
    }

    public void error(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.error(mapSupplier(messageSupplier), throwable);
    }

    public void error(String message) {
        log4jLogger.error(message);
    }

    public void error(String message, Throwable throwable) {
        log4jLogger.error(message, throwable);
    }

    public void error(String message, Object p0) {
        log4jLogger.error(message, p0);
    }

    public void error(String message, Object p0, Object p1) {
        log4jLogger.error(message, p0, p1);
    }

    public void error(String message, Object p0, Object p1, Object p2) {
        log4jLogger.error(message, p0, p1, p2);
    }

    public void error(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.error(message, p0, p1, p2, p3);
    }

    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.error(message, p0, p1, p2, p3, p4);
    }

    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.error(message, p0, p1, p2, p3, p4, p5);
    }

    public void error(String message, Object... params) {
        log4jLogger.error(message, params);
    }

    public void warn(Supplier<String> messageSupplier) {
        log4jLogger.warn(mapSupplier(messageSupplier));
    }

    public void warn(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.warn(mapSupplier(messageSupplier), throwable);
    }

    public void warn(String message) {
        log4jLogger.warn(message);
    }

    public void warn(String message, Throwable throwable) {
        log4jLogger.warn(message, throwable);
    }

    public void warn(String message, Object p0) {
        log4jLogger.warn(message, p0);
    }

    public void warn(String message, Object p0, Object p1) {
        log4jLogger.warn(message, p0, p1);
    }

    public void warn(String message, Object p0, Object p1, Object p2) {
        log4jLogger.warn(message, p0, p1, p2);
    }

    public void warn(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.warn(message, p0, p1, p2, p3);
    }

    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.warn(message, p0, p1, p2, p3, p4);
    }

    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.warn(message, p0, p1, p2, p3, p4, p5);
    }

    public void warn(String message, Object... params) {
        log4jLogger.warn(message, params);
    }

    public void info(Supplier<String> messageSupplier) {
        log4jLogger.info(mapSupplier(messageSupplier));
    }

    public void info(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.info(mapSupplier(messageSupplier), throwable);
    }

    public void info(String message) {
        log4jLogger.info(message);
    }

    public void info(String message, Throwable throwable) {
        log4jLogger.info(message, throwable);
    }

    public void info(String message, Object p0) {
        log4jLogger.info(message, p0);
    }

    public void info(String message, Object p0, Object p1) {
        log4jLogger.info(message, p0, p1);
    }

    public void info(String message, Object p0, Object p1, Object p2) {
        log4jLogger.info(message, p0, p1, p2);
    }

    public void info(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.info(message, p0, p1, p2, p3);
    }

    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.info(message, p0, p1, p2, p3, p4);
    }

    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.info(message, p0, p1, p2, p3, p4, p5);
    }

    public void info(String message, Object... params) {
        log4jLogger.info(message, params);
    }

    public void debug(Supplier<String> messageSupplier) {
        log4jLogger.debug(mapSupplier(messageSupplier));
    }

    public void debug(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.debug(mapSupplier(messageSupplier), throwable);
    }

    public void debug(String message) {
        log4jLogger.debug(message);
    }

    public void debug(String message, Throwable throwable) {
        log4jLogger.debug(message, throwable);
    }

    public void debug(String message, Object p0) {
        log4jLogger.debug(message, p0);
    }

    public void debug(String message, Object p0, Object p1) {
        log4jLogger.debug(message, p0, p1);
    }

    public void debug(String message, Object p0, Object p1, Object p2) {
        log4jLogger.debug(message, p0, p1, p2);
    }

    public void debug(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.debug(message, p0, p1, p2, p3);
    }

    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.debug(message, p0, p1, p2, p3, p4);
    }

    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.debug(message, p0, p1, p2, p3, p4, p5);
    }

    public void debug(String message, Object... params) {
        log4jLogger.debug(message, params);
    }

    public void trace(Supplier<String> messageSupplier) {
        log4jLogger.trace(mapSupplier(messageSupplier));
    }

    public void trace(Supplier<String> messageSupplier, Throwable throwable) {
        log4jLogger.trace(mapSupplier(messageSupplier), throwable);
    }

    public void trace(String message) {
        log4jLogger.trace(message);
    }

    public void trace(String message, Throwable throwable) {
        log4jLogger.trace(message, throwable);
    }

    public void trace(String message, Object p0) {
        log4jLogger.trace(message, p0);
    }

    public void trace(String message, Object p0, Object p1) {
        log4jLogger.trace(message, p0, p1);
    }

    public void trace(String message, Object p0, Object p1, Object p2) {
        log4jLogger.trace(message, p0, p1, p2);
    }

    public void trace(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.trace(message, p0, p1, p2, p3);
    }

    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.trace(message, p0, p1, p2, p3, p4);
    }

    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.trace(message, p0, p1, p2, p3, p4, p5);
    }

    public void trace(String message, Object... params) {
        log4jLogger.trace(message, params);
    }
}
