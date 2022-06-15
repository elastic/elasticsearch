/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;

/**
 * Represents a distributed tracing system that keeps track of the start and end of various activities in the cluster.
 */
public interface Tracer {

    /**
     * Called when the {@link Traceable} activity starts.
     * @param threadContext the current context. Required for tracing parent/child span activity.
     * @param traceable the thing to start tracing
     */
    void onTraceStarted(ThreadContext threadContext, Traceable traceable);

    /**
     * Called when the {@link Traceable} activity ends.
     * @param traceable the thing to stop tracing
     */
    void onTraceStopped(Traceable traceable);

    void onTraceEvent(Traceable traceable, String eventName);

    void onTraceException(Traceable traceable, Throwable throwable);

    void setAttribute(Traceable traceable, String key, boolean value);

    void setAttribute(Traceable traceable, String key, double value);

    void setAttribute(Traceable traceable, String key, long value);

    void setAttribute(Traceable traceable, String key, String value);

    Releasable withScope(Traceable traceable);

    /**
     * A Tracer implementation that does nothing. This is used when no tracer is configured,
     * in order to avoid null checks everywhere.
     */
    Tracer NOOP = new Tracer() {
        @Override
        public void onTraceStarted(ThreadContext threadContext, Traceable traceable) {}

        @Override
        public void onTraceStopped(Traceable traceable) {}

        @Override
        public void onTraceEvent(Traceable traceable, String eventName) {}

        @Override
        public void onTraceException(Traceable traceable, Throwable throwable) {}

        @Override
        public void setAttribute(Traceable traceable, String key, boolean value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, double value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, long value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, String value) {}

        @Override
        public Releasable withScope(Traceable traceable) {
            return () -> {};
        }
    };
}
