/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.sl4j.bridge;

import org.elasticsearch.logging.StandardLevels;
import org.slf4j.Logger;
import org.slf4j.Marker;

public class ESLogger implements Logger {

    private final org.elasticsearch.logging.Logger esLogger;

    public ESLogger(org.elasticsearch.logging.Logger esLogger) {
        this.esLogger = esLogger;
    }

    public String getName() {
        return esLogger.getName();
    }

    public boolean isTraceEnabled() {
        return esLogger.isTraceEnabled();
    }

    public void trace(String msg) {
        esLogger.trace(msg);
    }

    public void trace(String format, Object arg) {
        esLogger.trace(format, arg);
    }

    public void trace(String format, Object arg1, Object arg2) {
        esLogger.trace(format, arg1, arg2);
    }

    public void trace(String format, Object[] argArray) {
        esLogger.trace(format, argArray);
    }

    public void trace(String msg, Throwable t) {
        esLogger.trace(msg, t);
    }

    public boolean isTraceEnabled(Marker marker) {
        return esLogger.isTraceEnabled();
    }

    public void trace(Marker marker, String msg) {
        esLogger.trace(msg);
    }

    public void trace(Marker marker, String format, Object arg) {
        esLogger.trace(format, arg);
    }

    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        esLogger.trace(format, arg1, arg2);
    }

    public void trace(Marker marker, String format, Object[] argArray) {
        esLogger.trace(format, argArray);
    }

    public void trace(Marker marker, String msg, Throwable t) {
        esLogger.trace(msg, t);
    }

    public boolean isDebugEnabled() {
        return esLogger.isDebugEnabled();
    }

    public void debug(String msg) {
        esLogger.debug(msg);
    }

    public void debug(String format, Object arg) {
        esLogger.debug(format, arg);
    }

    public void debug(String format, Object arg1, Object arg2) {
        esLogger.debug(format, arg1, arg2);
    }

    public void debug(String format, Object[] argArray) {
        esLogger.debug(format, argArray);
    }

    public void debug(String msg, Throwable t) {
        esLogger.debug(msg, t);
    }

    public boolean isDebugEnabled(Marker marker) {
        return esLogger.isDebugEnabled();
    }

    public void debug(Marker marker, String msg) {
        esLogger.debug(msg);
    }

    public void debug(Marker marker, String format, Object arg) {
        esLogger.debug(format, arg);
    }

    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        esLogger.debug(format, arg1, arg2);
    }

    public void debug(Marker marker, String format, Object[] argArray) {
        esLogger.debug(format, argArray);
    }

    public void debug(Marker marker, String msg, Throwable t) {
        esLogger.debug(msg, t);
    }

    public boolean isInfoEnabled() {
        return esLogger.isInfoEnabled();
    }

    public void info(String msg) {
        esLogger.info(msg);
    }

    public void info(String format, Object arg) {
        esLogger.info(format, arg);
    }

    public void info(String format, Object arg1, Object arg2) {
        esLogger.info(format, arg1, arg2);
    }

    public void info(String format, Object[] argArray) {
        esLogger.info(format, argArray);
    }

    public void info(String msg, Throwable t) {
        esLogger.info(msg, t);
    }

    public boolean isInfoEnabled(Marker marker) {
        return esLogger.isInfoEnabled();
    }

    public void info(Marker marker, String msg) {
        esLogger.info(msg);
    }

    public void info(Marker marker, String format, Object arg) {
        esLogger.info(format, arg);
    }

    public void info(Marker marker, String format, Object arg1, Object arg2) {
        esLogger.info(format, arg1, arg2);
    }

    public void info(Marker marker, String format, Object[] argArray) {
        esLogger.info(format, argArray);
    }

    public void info(Marker marker, String msg, Throwable t) {
        esLogger.info(msg, t);
    }

    public boolean isWarnEnabled() {
        return esLogger.isWarnEnabled();
    }

    public void warn(String msg) {
        esLogger.warn(msg);
    }

    public void warn(String format, Object arg) {
        esLogger.warn(format, arg);
    }

    public void warn(String format, Object[] argArray) {
        esLogger.warn(format, argArray);
    }

    public void warn(String format, Object arg1, Object arg2) {
        esLogger.warn(format, arg1, arg2);
    }

    public void warn(String msg, Throwable t) {
        esLogger.warn(msg, t);
    }

    public boolean isWarnEnabled(Marker marker) {
        return esLogger.isWarnEnabled();
    }

    public void warn(Marker marker, String msg) {
        esLogger.warn(msg);
    }

    public void warn(Marker marker, String format, Object arg) {
        esLogger.warn(format, arg);
    }

    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        esLogger.warn(format, arg1, arg2);
    }

    public void warn(Marker marker, String format, Object[] argArray) {
        esLogger.warn(format, argArray);
    }

    public void warn(Marker marker, String msg, Throwable t) {
        esLogger.warn(msg, t);
    }

    public boolean isErrorEnabled() {
        return esLogger.isErrorEnabled();
    }

    public void error(String msg) {
        esLogger.error(msg);
    }

    public void error(String format, Object arg) {
        esLogger.error(format, arg);
    }

    public void error(String format, Object arg1, Object arg2) {
        esLogger.error(format, arg1, arg2);
    }

    public void error(String format, Object[] argArray) {
        esLogger.error(format, argArray);
    }

    public void error(String msg, Throwable t) {
        esLogger.error(msg, t);
    }

    public boolean isErrorEnabled(Marker marker) {
        return esLogger.isErrorEnabled();
    }

    public void error(Marker marker, String msg) {
        esLogger.error(msg);
    }

    public void error(Marker marker, String format, Object arg) {
        esLogger.error(format, arg);
    }

    public void error(Marker marker, String format, Object arg1, Object arg2) {
        esLogger.error(format, arg1, arg2);
    }

    public void error(Marker marker, String format, Object[] argArray) {
        esLogger.error(format, argArray);
    }

    public void error(Marker marker, String msg, Throwable t) {
        esLogger.error(msg, t);
    }

    public void log(Marker marker, String fqcn, int level, String message, Object[] argArray, Throwable t) {
        esLogger.log(/* fqcn, */ elasticsearchLevel(level), message, argArray, t);
    }

    public static org.elasticsearch.logging.Level elasticsearchLevel(final int level) {
        return switch (level) {
            case StandardLevels.OFF -> org.elasticsearch.logging.Level.OFF;
            case StandardLevels.FATAL -> org.elasticsearch.logging.Level.FATAL;
            case StandardLevels.ERROR -> org.elasticsearch.logging.Level.ERROR;
            case StandardLevels.WARN -> org.elasticsearch.logging.Level.WARN;
            case StandardLevels.INFO -> org.elasticsearch.logging.Level.INFO;
            case StandardLevels.DEBUG -> org.elasticsearch.logging.Level.DEBUG;
            case StandardLevels.TRACE -> org.elasticsearch.logging.Level.TRACE;
            case StandardLevels.ALL -> org.elasticsearch.logging.Level.ALL;
            default -> org.elasticsearch.logging.Level.ERROR;
        };
    }
}
