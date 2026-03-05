/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;

public class TimeSpanMarker {
    private TimeSpan timeSpan;
    private transient TimeSpan.Builder timeSpanBuilder;

    private final String name;
    private final boolean allowMultipleCalls;

    // Package private for testing
    TimeSpanMarker(String name, boolean allowMultipleCalls, TimeSpan timeSpan) {
        this.name = name;
        this.allowMultipleCalls = allowMultipleCalls;
        this.timeSpan = timeSpan;
    }

    public String name() {
        return name;
    }

    public TimeSpan timeSpan() {
        return timeSpan;
    }

    // visible for testing
    void timeSpan(TimeSpan timeSpan) {
        this.timeSpan = timeSpan;
    }

    public void start() {
        assert allowMultipleCalls || timeSpanBuilder == null : "start() should only be called once for " + name;
        if (timeSpanBuilder == null) {
            timeSpanBuilder = TimeSpan.start();
        }
    }

    public void stop() {
        assert timeSpanBuilder != null : "start() should have been called for " + name;
        assert allowMultipleCalls || timeSpan == null : "start() should only be called once for " + name;
        timeSpan = timeSpanBuilder.stop();
    }

    /**
     * Safely stops the marker only if it was started but not yet stopped.
     * This is useful in error paths where we don't know which markers were started.
     */
    public void stopIfStarted() {
        if (timeSpanBuilder != null && timeSpan == null) {
            timeSpan = timeSpanBuilder.stop();
        }
    }

    public TimeValue timeTook() {
        return timeSpan == null ? null : timeSpan.toTimeValue();
    }

    /**
     * Returns true if this marker was started (regardless of whether it was stopped).
     */
    public boolean wasStarted() {
        return timeSpanBuilder != null;
    }

    public TimeValue timeSinceStarted() {
        return timeSpanBuilder != null ? timeSpanBuilder.stop().toTimeValue() : TimeValue.ZERO;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TimeSpanMarker that = (TimeSpanMarker) o;
        return allowMultipleCalls == that.allowMultipleCalls && Objects.equals(timeSpan, that.timeSpan) && Objects.equals(name, that.name);
        // Don't consider timeStampBuilders for equality
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSpan, name, allowMultipleCalls);
    }

    @Override
    public String toString() {
        return "TimeSpanMarker{" + "name='" + name + '\'' + ", timeSpan=" + timeSpan + ", allowMultipleCalls=" + allowMultipleCalls + '}';
    }
}
