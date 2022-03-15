/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.apache.logging.log4j.core.time.Instant;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Message;

import java.util.Map;

public class LogEventImpl implements org.elasticsearch.logging.api.core.LogEvent {

    private LogEvent logEvent;

    public LogEventImpl(LogEvent log4jLogEvent) {
        this.logEvent = log4jLogEvent;
    }

    public LogEvent toImmutable() {
        return logEvent.toImmutable();
    }

    public Map<String, String> getContextMap() {
        return logEvent.getContextMap();
    }

    public ReadOnlyStringMap getContextData() {
        return logEvent.getContextData();
    }

    public ThreadContext.ContextStack getContextStack() {
        return logEvent.getContextStack();
    }

    public String getLoggerFqcn() {
        return logEvent.getLoggerFqcn();
    }

    public Level getLevel() {
        return Util.elasticsearchLevel(logEvent.getLevel());
    }

    public String getLoggerName() {
        return logEvent.getLoggerName();
    }

    public Marker getMarker() {
        return logEvent.getMarker();
    }

    public String getMarkerName() {
        return logEvent.getMarker() != null ? logEvent.getMarker().getName() : null;
    }

    public Message getMessage() {
        return new MessageImpl(logEvent.getMessage());
    }

    public long getTimeMillis() {
        return logEvent.getTimeMillis();
    }

    public Instant getInstant() {
        return logEvent.getInstant();
    }

    public StackTraceElement getSource() {
        return logEvent.getSource();
    }

    public String getThreadName() {
        return logEvent.getThreadName();
    }

    public long getThreadId() {
        return logEvent.getThreadId();
    }

    public int getThreadPriority() {
        return logEvent.getThreadPriority();
    }

    public Throwable getThrown() {
        return logEvent.getThrown();
    }

    public ThrowableProxy getThrownProxy() {
        return logEvent.getThrownProxy();
    }

    public boolean isEndOfBatch() {
        return logEvent.isEndOfBatch();
    }

    public boolean isIncludeLocation() {
        return logEvent.isIncludeLocation();
    }

    public void setEndOfBatch(boolean endOfBatch) {
        logEvent.setEndOfBatch(endOfBatch);
    }

    public void setIncludeLocation(boolean locationRequired) {
        logEvent.setIncludeLocation(locationRequired);
    }

    public long getNanoTime() {
        return logEvent.getNanoTime();
    }

}
