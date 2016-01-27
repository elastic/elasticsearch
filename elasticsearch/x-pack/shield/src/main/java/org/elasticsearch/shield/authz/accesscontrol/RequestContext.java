/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;

import java.util.Objects;

/**
 * A thread local based holder of the currnet {@link TransportRequest} instance.
 */
public final class RequestContext {

    // Need thread local to make the current transport request available to places in the code that
    // don't have direct access to the current transport request
    private static final ThreadLocal<RequestContext> current = new ThreadLocal<>();

    /**
     * If set then this returns the current {@link RequestContext} with the current {@link TransportRequest}.
     */
    public static RequestContext current() {
        return current.get();
    }

    /**
     * Invoked by the transport service to set the current transport request in the thread local
     */
    public static void setCurrent(RequestContext value) {
        current.set(value);
    }

    /**
     * Invoked by the transport service to remove the current request from the thread local
     */
    public static void removeCurrent() {
        current.remove();
    }

    private final ThreadContext threadContext;
    private final TransportRequest request;

    public RequestContext(TransportRequest request, ThreadContext threadContext) {
        this.request = Objects.requireNonNull(request);
        this.threadContext = Objects.requireNonNull(threadContext);
    }

    /**
     * @return current {@link TransportRequest}
     */
    public TransportRequest getRequest() {
        return request;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }
}
