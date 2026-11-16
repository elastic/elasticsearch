/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import java.util.Map;

/**
 * Represents a distributed tracing system that keeps track of the start and end
 * of various activities in the cluster. Traces are composed of "spans", each of
 * which represents some piece of work with a duration. Spans are nested to
 * create a parent-child hierarchy.
 * <p>
 * You can open a span using {@link #startTrace}, and stop it using {@link #stopTrace}, at which point the tracing system will queue
 * the data to be sent somewhere for processing and storage.
 * <p>
 * You can add additional data to a span using the {@code #setAttribute} methods. This allows you
 * to attach data that is not available when the span is opened.
 * <p>
 * <b>Implementation node:</b> tracers have similar performance requirements to loggers,
 * in that they may be called in performance-sensitive locations. For example, network
 * requests must be queued on separate threads, in order to avoid blocking any
 * calling threads. Care should be taken to avoid allocating an excessive number of
 * objects.
 */
public interface Tracer {

    /**
     * Called when a span starts.
     * @param traceContext the current context. Required for tracing parent/child span activity.
     * @param traceable provides a unique identifier for the activity, and will not be sent to the tracing system. Add the ID
     *                  to the attributes if it is important
     * @param name the name of the span. Used to filter out spans, but also sent to the tracing system
     * @param attributes arbitrary key/value data for the span. Sent to the tracing system
     */
    void startTrace(TraceContext traceContext, Traceable traceable, String name, Map<String, Object> attributes);

    /**
     * Called when a span ends.
     * @param traceable provides an identifier for the span
     */
    void stopTrace(Traceable traceable);

    /**
     * If an exception occurs during a span, you can add data about the exception to the span where the exception occurred.
     * This should only be called when a span has been started, otherwise it has no effect.
     * @param traceable provides an identifier for the span
     * @param throwable the exception that occurred.
     */
    void addError(Traceable traceable, Throwable throwable);

    /**
     * Adds a boolean attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param traceable provides an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(Traceable traceable, String key, boolean value);

    /**
     * Adds a double attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param traceable provides an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(Traceable traceable, String key, double value);

    /**
     * Adds a long attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param traceable provides an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(Traceable traceable, String key, long value);

    /**
     * Adds a String attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param traceable provides an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(Traceable traceable, String key, String value);

    /**
     * A Tracer implementation that does nothing. This is used when no tracer is configured,
     * in order to avoid null checks everywhere.
     */
    Tracer NOOP = new Tracer() {
        @Override
        public void startTrace(TraceContext traceContext, Traceable traceable, String name, Map<String, Object> attributes) {}

        @Override
        public void stopTrace(Traceable traceable) {}

        @Override
        public void addError(Traceable traceable, Throwable throwable) {}

        @Override
        public void setAttribute(Traceable traceable, String key, boolean value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, double value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, long value) {}

        @Override
        public void setAttribute(Traceable traceable, String key, String value) {}
    };

    interface AttributeKeys {
        String TASK_ID = "es.task.id";
        String PARENT_TASK_ID = "es.task.parent.id";
        String CLUSTER_NAME = "es.cluster.name";
        String NODE_NAME = "es.node.name";
    }
}
