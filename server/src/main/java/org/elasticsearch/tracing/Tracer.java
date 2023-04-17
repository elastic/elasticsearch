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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;

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
     * @param threadContext the current context. Required for tracing parent/child span activity.
     * @param spanId a unique identifier for the activity, and will not be sent to the tracing system. Add the ID
     *               to the attributes if it is important
     * @param name the name of the span. Used to filter out spans, but also sent to the tracing system
     * @param attributes arbitrary key/value data for the span. Sent to the tracing system
     */
    void startTrace(ThreadContext threadContext, SpanId spanId, String name, Map<String, Object> attributes);

    /**
     * @see Tracer#startTrace(ThreadContext, SpanId, String, Map)
     */
    default void startTrace(ThreadContext threadContext, Task task, String name, Map<String, Object> attributes) {
        startTrace(threadContext, SpanId.forTask(task), name, attributes);
    }

    /**
     * @see Tracer#startTrace(ThreadContext, SpanId, String, Map)
     */
    default void startTrace(ThreadContext threadContext, RestRequest restRequest, String name, Map<String, Object> attributes) {
        startTrace(threadContext, SpanId.forRestRequest(restRequest), name, attributes);
    }

    /**
     * Called when a span starts. This version of the method relies on context to assign the span a parent.
     * @param name the name of the span. Sent to the tracing system
     */
    void startTrace(String name, Map<String, Object> attributes);

    /**
     * Called when a span ends.
     * @param spanId an identifier for the span
     */
    void stopTrace(SpanId spanId);

    /**
     * @see Tracer#stopTrace(SpanId)
     */
    default void stopTrace(Task task) {
        stopTrace(SpanId.forTask(task));
    }

    /**
     * @see Tracer#stopTrace(SpanId)
     */
    default void stopTrace(RestRequest restRequest) {
        stopTrace(SpanId.forRestRequest(restRequest));
    }

    /**
     * Called when a span ends. This version of the method relies on context to select the span to stop.
     */
    void stopTrace();

    /**
     * Some tracing implementations support the concept of "events" within a span, marking a point in time during the span
     * when something interesting happened. If the tracing implementation doesn't support events, then nothing will be recorded.
     * This should only be called when a trace already been started on the {@code traceable}.
     * @param spanId an identifier for the span
     * @param eventName the event that happened. This should be something meaningful to people reviewing the data, for example
     *                  "send response", "finished processing", "validated request", etc.
     */
    void addEvent(SpanId spanId, String eventName);

    /**
     * If an exception occurs during a span, you can add data about the exception to the span where the exception occurred.
     * This should only be called when a span has been started, otherwise it has no effect.
     * @param spanId an identifier for the span
     * @param throwable the exception that occurred.
     */
    void addError(SpanId spanId, Throwable throwable);

    /**
     * @see Tracer#addError(SpanId, Throwable)
     */
    default void addError(RestRequest restRequest, Throwable throwable) {
        addError(SpanId.forRestRequest(restRequest), throwable);
    }

    /**
     * Adds a boolean attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param spanId an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(SpanId spanId, String key, boolean value);

    /**
     * Adds a double attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param spanId an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(SpanId spanId, String key, double value);

    /**
     * Adds a long attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param spanId an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(SpanId spanId, String key, long value);

    /**
     * Adds a String attribute to an active span. These will be sent to the endpoint that collects tracing data.
     * @param spanId an identifier for the span
     * @param key the attribute key
     * @param value the attribute value
     */
    void setAttribute(SpanId spanId, String key, String value);

    /**
     * Usually you won't need this about scopes when using tracing. However,
     * sometimes you may want more details to be captured about a particular
     * section of code. You can think of "scope" as representing the currently active
     * tracing context. Using scope allows the tracing agent to do the following:
     *
     * <ul>
     *   <li>Enables automatic correlation between the "active span" and logging, where logs have also been captured.
     *   <li>Enables capturing any exceptions thrown when the span is active, and linking those exceptions to the span.
     *   <li>Allows the sampling profiler to be used as it allows samples to be linked to the active span (if any), so the agent can
     *   automatically get extra spans without manual instrumentation.
     * </ul>
     *
     * <p>However, a scope must be closed in the same thread in which it was opened, which
     * cannot be guaranteed when using tasks.
     *
     * <p>Note that in the OpenTelemetry documentation, spans, scope and context are fairly
     * straightforward to use, since `Scope` is an `AutoCloseable` and so can be
     * easily created and cleaned up use try-with-resources blocks. Unfortunately,
     * Elasticsearch is a complex piece of software, and also extremely asynchronous,
     * so the typical OpenTelemetry examples do not work.
     *
     * <p>Nonetheless, it is possible to manually use scope where more detail is needed by
     * explicitly opening a scope via the `Tracer`.
     *
     * @param spanId an identifier for the span
     * @return a scope. You MUST close it when you are finished with it.
     */
    Releasable withScope(SpanId spanId);

    /**
     * @see Tracer#withScope(SpanId)
     */
    default Releasable withScope(Task task) {
        return withScope(SpanId.forTask(task));
    }

    /**
     * A Tracer implementation that does nothing. This is used when no tracer is configured,
     * in order to avoid null checks everywhere.
     */
    Tracer NOOP = new Tracer() {
        @Override
        public void startTrace(ThreadContext threadContext, SpanId spanId, String name, Map<String, Object> attributes) {}

        @Override
        public void startTrace(ThreadContext threadContext, Task task, String name, Map<String, Object> attributes) {}

        @Override
        public void startTrace(ThreadContext threadContext, RestRequest restRequest, String name, Map<String, Object> attributes) {}

        @Override
        public void startTrace(String name, Map<String, Object> attributes) {}

        @Override
        public void stopTrace(SpanId spanId) {}

        @Override
        public void stopTrace(Task task) {}

        @Override
        public void stopTrace(RestRequest restRequest) {}

        @Override
        public void stopTrace() {}

        @Override
        public void addEvent(SpanId spanId, String eventName) {}

        @Override
        public void addError(SpanId spanId, Throwable throwable) {}

        @Override
        public void addError(RestRequest restRequest, Throwable throwable) {}

        @Override
        public void setAttribute(SpanId spanId, String key, boolean value) {}

        @Override
        public void setAttribute(SpanId spanId, String key, double value) {}

        @Override
        public void setAttribute(SpanId spanId, String key, long value) {}

        @Override
        public void setAttribute(SpanId spanId, String key, String value) {}

        @Override
        public Releasable withScope(SpanId spanId) {
            return () -> {};
        }

        @Override
        public Releasable withScope(Task task) {
            return () -> {};
        }
    };

    interface AttributeKeys {
        String TASK_ID = "es.task.id";
        String PARENT_TASK_ID = "es.task.parent.id";
        String CLUSTER_NAME = "es.cluster.name";
        String NODE_NAME = "es.node.name";
    }
}
