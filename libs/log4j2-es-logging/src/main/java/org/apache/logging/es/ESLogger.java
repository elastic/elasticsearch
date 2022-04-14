/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.logging.es;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.elasticsearch.logging.Logger;

public class ESLogger extends AbstractLogger {
    private final Logger esLogger;

    public ESLogger(String name, org.elasticsearch.logging.Logger esLogger) {
        super(name);
        this.esLogger = esLogger;
    }

    public ESLogger(String name, MessageFactory messageFactory, org.elasticsearch.logging.Logger esLogger) {
        super(name, messageFactory);
        this.esLogger = esLogger;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, Message message, Throwable t) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, CharSequence message, Throwable t) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, Object message, Throwable t) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Throwable t) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object... params) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(
        Level level,
        Marker marker,
        String message,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6
    ) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(
        Level level,
        Marker marker,
        String message,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7
    ) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(
        Level level,
        Marker marker,
        String message,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7,
        Object p8
    ) {
        return isEnabledFor(level);
    }

    @Override
    public boolean isEnabled(
        Level level,
        Marker marker,
        String message,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7,
        Object p8,
        Object p9
    ) {
        return isEnabledFor(level);
    }

    private boolean isEnabledFor(Level level) {
        return switch (level.getStandardLevel()) {
            case WARN -> esLogger.isWarnEnabled();
            case INFO -> esLogger.isInfoEnabled();
            case DEBUG -> esLogger.isDebugEnabled();
            case TRACE -> esLogger.isTraceEnabled();
            default -> esLogger.isErrorEnabled();
        };
    }

    @Override
    public void logMessage(String fqcn, Level level, Marker marker, Message message, Throwable t) {
        String formattedMessage = message.getFormattedMessage();
        switch (level.getStandardLevel()) {
            case DEBUG:
                esLogger.debug(formattedMessage, t);
                break;
            case TRACE:
                esLogger.trace(formattedMessage, t);
                break;
            case INFO:
                esLogger.info(formattedMessage, t);
                break;
            case WARN:
                esLogger.warn(formattedMessage, t);
                break;
            case ERROR:
                esLogger.error(formattedMessage, t);
                break;
            default:
                esLogger.error(formattedMessage, t);
                break;
        }
    }

    @Override
    public Level getLevel() {
        if (esLogger.isTraceEnabled()) {
            return Level.TRACE;
        }
        if (esLogger.isDebugEnabled()) {
            return Level.DEBUG;
        }
        if (esLogger.isInfoEnabled()) {
            return Level.INFO;
        }
        if (esLogger.isWarnEnabled()) {
            return Level.WARN;
        }
        if (esLogger.isErrorEnabled()) {
            return Level.ERROR;
        }
        // Option: throw new IllegalStateException("Unknown SLF4JLevel");
        // Option: return Level.ALL;
        return Level.OFF;
    }
}
