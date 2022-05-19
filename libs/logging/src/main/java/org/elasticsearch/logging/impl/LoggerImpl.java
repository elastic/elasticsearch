/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

import java.util.function.Supplier;

public final class LoggerImpl implements Logger {
    private final org.apache.logging.log4j.Logger log4jLogger;

    //
    public LoggerImpl(org.apache.logging.log4j.Logger log4jLogger) {
        this.log4jLogger = log4jLogger;
    }

    /*package*/ org.apache.logging.log4j.Logger log4jLogger() {
        return log4jLogger;
    }

    private static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return Util.log4jLevel(level);
    }

    @Override
    public void log(Level level, Object message) {
        log4jLogger.log(log4jLevel(level), message);
    }

    @Override
    public void log(Level level, Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.log(log4jLevel(level), msgSupplier, thrown);
    }

    public Level getLevel() {
        return Util.elasticsearchLevel(log4jLogger.getLevel());
    }

    public String getName() {
        return log4jLogger.getName();
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
        return log4jLogger.isEnabled(log4jLevel(level));
    }

    @Override
    public void fatal(Object message) {
        log4jLogger.fatal(message);
    }

    @Override
    public void fatal(Object message, Throwable thrown) {
        log4jLogger.fatal(message, thrown);
    }

    @Override
    public void fatal(Supplier<String> msgSupplier) {
        log4jLogger.fatal(msgSupplier);
    }

    @Override
    public void fatal(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.fatal(msgSupplier, thrown);
    }

    @Override
    public void fatal(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.fatal(messagePattern, paramSupplier);
    }

    @Override
    public void fatal(String message) {
        log4jLogger.fatal(message);
    }

    @Override
    public void fatal(String message, Throwable thrown) {
        log4jLogger.fatal(message, thrown);
    }

    @Override
    public void fatal(String message, Object p0) {
        log4jLogger.fatal(message, p0);
    }

    @Override
    public void fatal(String message, Object p0, Object p1) {
        log4jLogger.fatal(message, p0, p1);
    }

    @Override
    public void fatal(String message, Object p0, Object p1, Object p2) {
        log4jLogger.fatal(message, p0, p1, p2);
    }

    @Override
    public void fatal(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.fatal(message, p0, p1, p2, p3);
    }

    @Override
    public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.fatal(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.fatal(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void fatal(String message, Object... params) {
        log4jLogger.fatal(message, params);
    }

    @Override
    public void error(Object message) {
        log4jLogger.error(message);
    }

    @Override
    public void error(Object message, Throwable thrown) {
        log4jLogger.error(message, thrown);
    }

    @Override
    public void error(Supplier<String> msgSupplier) {
        log4jLogger.error(msgSupplier);
    }

    @Override
    public void error(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.error(msgSupplier, thrown);
    }

    @Override
    public void error(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.error(messagePattern, paramSupplier);
    }

    @Override
    public void error(String message) {
        log4jLogger.error(message);
    }

    @Override
    public void error(String message, Throwable thrown) {
        log4jLogger.error(message, thrown);
    }

    @Override
    public void error(String message, Object p0) {
        log4jLogger.error(message, p0);
    }

    @Override
    public void error(String message, Object p0, Object p1) {
        log4jLogger.error(message, p0, p1);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2) {
        log4jLogger.error(message, p0, p1, p2);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.error(message, p0, p1, p2, p3);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.error(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.error(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void error(String message, Object... params) {
        log4jLogger.error(message, params);
    }

    @Override
    public void warn(Object message) {
        log4jLogger.warn(message);
    }

    @Override
    public void warn(Object message, Throwable thrown) {
        log4jLogger.warn(message, thrown);
    }

    @Override
    public void warn(Supplier<String> msgSupplier) {
        log4jLogger.warn(msgSupplier);
    }

    @Override
    public void warn(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.warn(msgSupplier, thrown);
    }

    @Override
    public void warn(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.warn(messagePattern, paramSupplier);
    }

    @Override
    public void warn(String message) {
        log4jLogger.warn(message);
    }

    @Override
    public void warn(String message, Throwable thrown) {
        log4jLogger.warn(message, thrown);
    }

    @Override
    public void warn(String message, Object p0) {
        log4jLogger.warn(message, p0);
    }

    @Override
    public void warn(String message, Object p0, Object p1) {
        log4jLogger.warn(message, p0, p1);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2) {
        log4jLogger.warn(message, p0, p1, p2);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.warn(message, p0, p1, p2, p3);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.warn(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.warn(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void warn(String message, Object... params) {
        log4jLogger.warn(message, params);
    }

    @Override
    public void info(Object message) {
        log4jLogger.info(message);
    }

    @Override
    public void info(Object message, Throwable thrown) {
        log4jLogger.info(message, thrown);
    }

    @Override
    public void info(Supplier<String> msgSupplier) {
        log4jLogger.info(msgSupplier);
    }

    @Override
    public void info(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.info(msgSupplier, thrown);
    }

    @Override
    public void info(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.info(messagePattern, paramSupplier);
    }

    @Override
    public void info(String message) {
        log4jLogger.info(message);
    }

    @Override
    public void info(String message, Throwable thrown) {
        log4jLogger.info(message, thrown);
    }

    @Override
    public void info(String message, Object p0) {
        log4jLogger.info(message, p0);
    }

    @Override
    public void info(String message, Object p0, Object p1) {
        log4jLogger.info(message, p0, p1);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2) {
        log4jLogger.info(message, p0, p1, p2);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.info(message, p0, p1, p2, p3);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.info(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.info(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void info(String message, Object... params) {
        log4jLogger.info(message, params);
    }

    @Override
    public void debug(Object message) {
        log4jLogger.debug(message);
    }

    @Override
    public void debug(Object message, Throwable thrown) {
        log4jLogger.debug(message, thrown);
    }

    @Override
    public void debug(Supplier<String> msgSupplier) {
        log4jLogger.debug(msgSupplier);
    }

    @Override
    public void debug(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.debug(msgSupplier, thrown);
    }

    @Override
    public void debug(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.debug(messagePattern, paramSupplier);
    }

    @Override
    public void debug(String message) {
        log4jLogger.debug(message);
    }

    @Override
    public void debug(String message, Throwable thrown) {
        log4jLogger.debug(message, thrown);
    }

    @Override
    public void debug(String message, Object p0) {
        log4jLogger.debug(message, p0);
    }

    @Override
    public void debug(String message, Object p0, Object p1) {
        log4jLogger.debug(message, p0, p1);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2) {
        log4jLogger.debug(message, p0, p1, p2);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.debug(message, p0, p1, p2, p3);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.debug(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.debug(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void debug(String message, Object... params) {
        log4jLogger.debug(message, params);
    }

    @Override
    public void trace(Object message) {
        log4jLogger.trace(message);
    }

    @Override
    public void trace(Object message, Throwable thrown) {
        log4jLogger.trace(message, thrown);
    }

    @Override
    public void trace(Supplier<String> msgSupplier) {
        log4jLogger.trace(msgSupplier);
    }

    @Override
    public void trace(Supplier<String> msgSupplier, Throwable thrown) {
        log4jLogger.trace(msgSupplier, thrown);
    }

    @Override
    public void trace(String messagePattern, Supplier<String> paramSupplier) {
        log4jLogger.trace(messagePattern, paramSupplier);
    }

    @Override
    public void trace(String message) {
        log4jLogger.trace(message);
    }

    @Override
    public void trace(String message, Throwable thrown) {
        log4jLogger.trace(message, thrown);
    }

    @Override
    public void trace(String message, Object p0) {
        log4jLogger.trace(message, p0);
    }

    @Override
    public void trace(String message, Object p0, Object p1) {
        log4jLogger.trace(message, p0, p1);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2) {
        log4jLogger.trace(message, p0, p1, p2);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3) {
        log4jLogger.trace(message, p0, p1, p2, p3);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log4jLogger.trace(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log4jLogger.trace(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void trace(String message, Object... params) {
        log4jLogger.trace(message, params);
    }
}
