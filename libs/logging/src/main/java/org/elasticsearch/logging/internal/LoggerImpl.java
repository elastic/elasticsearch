/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Message;

import java.util.function.Supplier;

public final class LoggerImpl implements org.elasticsearch.logging.Logger {
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

    public Level getLevel() {
        return Util.elasticsearchLevel(log4jLogger.getLevel());
    }

    public String getName() {
        return log4jLogger.getName();
    }

    public boolean isInfoEnabled() {
        return log4jLogger.isInfoEnabled();
    }

    public boolean isTraceEnabled() {
        return log4jLogger.isTraceEnabled();
    }

    public boolean isDebugEnabled() {
        return log4jLogger.isDebugEnabled();
    }

    public boolean isErrorEnabled() {
        return log4jLogger.isErrorEnabled();
    }

    public boolean isWarnEnabled() {
        return log4jLogger.isWarnEnabled();
    }

    @Override
    public void log(Level level, Message message) {
        log4jLogger.log(log4jLevel(level), message);
    }

    public void debug(Message message) {
        log4jLogger.debug(message);
    }

    public void debug(Message message, Throwable thrown) {
        log4jLogger.debug(message, thrown);
    }

    public void debug(Supplier<?> msgSupplier, Throwable thrown) {
        log4jLogger.debug(msgSupplier, thrown);
    }

    public void debug(String messagePattern, Supplier<?> paramSupplier) {
        log4jLogger.debug(messagePattern, paramSupplier);
    }

    public void debug(String message) {
        log4jLogger.debug(message);
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

    public void debug(Supplier<?> msgSupplier) {
        log4jLogger.debug(msgSupplier);
    }

    public void error(Message message) {
        log4jLogger.error(message);
    }

    public void error(Message message, Throwable thrown) {
        log4jLogger.error(message, thrown);
    }

    public void error(Supplier<?> msgSupplier) {
        log4jLogger.error(msgSupplier);
    }

    public void error(Supplier<?> msgSupplier, Throwable thrown) {
        log4jLogger.error(msgSupplier, thrown);
    }

    public void error(String message) {
        log4jLogger.error(message);
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

    public void info(Message message) {
        log4jLogger.info(message);
    }

    public void info(Message message, Throwable thrown) {
        log4jLogger.info(message, thrown);
    }

    public void info(Supplier<?> msgSupplier) {
        log4jLogger.info(msgSupplier);
    }

    public void info(Supplier<?> msgSupplier, Throwable thrown) {
        log4jLogger.info(msgSupplier, thrown);
    }

    public void info(String message) {
        log4jLogger.info(message);
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

    public void trace(Message message) {
        log4jLogger.trace(message);
    }

    public void trace(Message message, Throwable thrown) {
        log4jLogger.trace(message, thrown);
    }

    public void trace(Supplier<?> msgSupplier) {
        log4jLogger.trace(msgSupplier);
    }

    public void trace(Supplier<?> msgSupplier, Throwable thrown) {
        log4jLogger.trace(msgSupplier, thrown);
    }

    public void trace(String message) {
        log4jLogger.trace(message);
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

    public void warn(Message message) {
        log4jLogger.warn(message);
    }

    public void warn(Message message, Throwable thrown) {
        log4jLogger.warn(message, thrown);
    }

    public void warn(Supplier<?> msgSupplier) {
        log4jLogger.warn(msgSupplier);
    }

    public void warn(Supplier<?> msgSupplier, Throwable thrown) {
        log4jLogger.warn(msgSupplier, thrown);
    }

    public void warn(String message) {
        log4jLogger.warn(message);
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

    public void fatal(String message, Throwable thrown) {
        log4jLogger.fatal(message, thrown);
    }


}
