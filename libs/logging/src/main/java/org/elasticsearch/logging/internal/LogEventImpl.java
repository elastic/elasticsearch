/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side @Override
public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side @Override
public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.apache.logging.log4j.core.time.Instant;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import java.io.Serializable;
import java.util.Map;

public class LogEventImpl implements Serializable, LogEvent, org.elasticsearch.logging.api.core.LogEvent {

    private LogEvent logEvent;

    public LogEventImpl(LogEvent log4jLogEvent) {
        this.logEvent = log4jLogEvent;
    }

    @Override
    public LogEvent toImmutable() {
        return logEvent.toImmutable();
    }

    @Override
    public Map<String, String> getContextMap() {
        return logEvent.getContextMap();
    }

    @Override
    public ReadOnlyStringMap getContextData() {
        return logEvent.getContextData();
    }

    @Override
    public ThreadContext.ContextStack getContextStack() {
        return logEvent.getContextStack();
    }

    @Override
    public String getLoggerFqcn() {
        return logEvent.getLoggerFqcn();
    }

    @Override
    public Level getLevel() {
        return logEvent.getLevel();
    }

    @Override
    public String getLoggerName() {
        return logEvent.getLoggerName();
    }

    @Override
    public Marker getMarker() {
        return logEvent.getMarker();
    }

    @Override
    public Message getMessage() {
        return logEvent.getMessage();
    }

    @Override
    public long getTimeMillis() {
        return logEvent.getTimeMillis();
    }

    @Override
    public Instant getInstant() {
        return logEvent.getInstant();
    }

    @Override
    public StackTraceElement getSource() {
        return logEvent.getSource();
    }

    @Override
    public String getThreadName() {
        return logEvent.getThreadName();
    }

    @Override
    public long getThreadId() {
        return logEvent.getThreadId();
    }

    @Override
    public int getThreadPriority() {
        return logEvent.getThreadPriority();
    }

    @Override
    public Throwable getThrown() {
        return logEvent.getThrown();
    }

    @Override
    public ThrowableProxy getThrownProxy() {
        return logEvent.getThrownProxy();
    }

    @Override
    public boolean isEndOfBatch() {
        return logEvent.isEndOfBatch();
    }

    @Override
    public boolean isIncludeLocation() {
        return logEvent.isIncludeLocation();
    }

    @Override
    public void setEndOfBatch(boolean endOfBatch) {
        logEvent.setEndOfBatch(endOfBatch);
    }

    @Override
    public void setIncludeLocation(boolean locationRequired) {
        logEvent.setIncludeLocation(locationRequired);
    }

    @Override
    public long getNanoTime() {
        return logEvent.getNanoTime();
    }


}
