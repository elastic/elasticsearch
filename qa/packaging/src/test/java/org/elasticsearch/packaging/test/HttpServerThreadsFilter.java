/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The JDK's {@link com.sun.net.httpserver.HttpServer} (used by our {@code MockWebServer}) eagerly starts daemon
 * {@link java.util.Timer} threads in {@code sun.net.httpserver.ServerImpl}, named {@code idle-timeout-task} and
 * {@code req-rsp-timeout-task}. Stopping the server cancels these timers but does not join their threads, so they
 * can still be terminating when per-test thread leak detection runs, producing a spurious {@code ThreadLeakError}.
 * Filter them out since we can't deterministically wait for the JDK to finish tearing them down.
 */
public class HttpServerThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        final String name = t.getName();
        return name.equals("idle-timeout-task") || name.equals("req-rsp-timeout-task");
    }
}
