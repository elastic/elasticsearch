/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.tasks.Task;

import java.util.Set;

public enum Transports {
    ;
    private static final Set<String> REQUEST_HEADERS_ALLOWED_ON_DEFAULT_THREAD_CONTEXT = Set.of(
        Task.TRACE_ID,
        Task.TRACE_PARENT,
        Task.X_OPAQUE_ID_HTTP_HEADER,
        Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER
    );

    /** threads whose name is prefixed by this string will be considered network threads, even though they aren't */
    public static final String TEST_MOCK_TRANSPORT_THREAD_PREFIX = "__mock_network_thread";

    private static final String[] TRANSPORT_THREAD_NAMES = new String[] {
        '[' + HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX + ']',
        '[' + TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX + ']',
        TEST_MOCK_TRANSPORT_THREAD_PREFIX };

    /**
     * Utility method to detect whether a thread is a network thread. Typically
     * used in assertions to make sure that we do not call blocking code from
     * networking threads.
     */
    public static boolean isTransportThread(Thread t) {
        return isTransportThread(t.getName());
    }

    /**
     * Utility method to detect whether a thread is a network thread. Typically
     * used in assertions to make sure that we do not call blocking code from
     * networking threads.
     * @param threadName the name of the thread
     */
    public static boolean isTransportThread(String threadName) {
        for (String s : TRANSPORT_THREAD_NAMES) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }

    public static boolean assertTransportThread() {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) : "Expected transport thread but got [" + t + "]";
        return true;
    }

    public static boolean assertNotTransportThread(String reason) {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) == false : "Expected current thread [" + t + "] to not be a transport thread. Reason: [" + reason + "]";
        return true;
    }

    public static boolean assertDefaultThreadContext(ThreadContext threadContext) {
        assert threadContext.getRequestHeadersOnly().isEmpty()
            || REQUEST_HEADERS_ALLOWED_ON_DEFAULT_THREAD_CONTEXT.containsAll(threadContext.getRequestHeadersOnly().keySet())
            : "expected empty context but was " + threadContext.getRequestHeadersOnly() + " on " + Thread.currentThread().getName();
        return true;
    }
}
