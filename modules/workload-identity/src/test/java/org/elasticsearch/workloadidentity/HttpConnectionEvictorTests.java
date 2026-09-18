/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lifecycle tests for {@link HttpConnectionEvictor}. Uses a {@link DeterministicTaskQueue}
 * so eviction passes can be advanced synchronously, and a small in-test {@link NHttpClientConnectionManager}
 * stub that records the eviction calls we care about and rejects everything else.
 */
public class HttpConnectionEvictorTests extends ESTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(30);
    private static final TimeValue MAX_IDLE = TimeValue.timeValueSeconds(60);

    public void testStartSchedulesPeriodicEviction() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingConnectionManager manager = new RecordingConnectionManager();

        try (var evictor = new HttpConnectionEvictor(taskQueue.getThreadPool(), manager, INTERVAL, MAX_IDLE)) {
            evictor.start();
            assertEquals("eviction must not run synchronously on start()", 0, manager.closeExpiredCalls.get());

            advanceOneInterval(taskQueue);
            assertEquals(1, manager.closeExpiredCalls.get());
            assertEquals(1, manager.closeIdleCalls.get());
            assertEquals(MAX_IDLE.millis(), manager.lastIdleMillis.get());

            advanceOneInterval(taskQueue);
            assertEquals("eviction must reschedule itself for the next interval", 2, manager.closeExpiredCalls.get());
            assertEquals(2, manager.closeIdleCalls.get());
        }
    }

    public void testStartIsIdempotent() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingConnectionManager manager = new RecordingConnectionManager();

        try (var evictor = new HttpConnectionEvictor(taskQueue.getThreadPool(), manager, INTERVAL, MAX_IDLE)) {
            evictor.start();
            evictor.start();

            advanceOneInterval(taskQueue);
            assertEquals("a duplicate start() must not schedule a second eviction task", 1, manager.closeExpiredCalls.get());
        }
    }

    public void testIsRunningReflectsLifecycle() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingConnectionManager manager = new RecordingConnectionManager();

        final var evictor = new HttpConnectionEvictor(taskQueue.getThreadPool(), manager, INTERVAL, MAX_IDLE);
        try {
            assertFalse("evictor must not report running before start()", evictor.isRunning());
            evictor.start();
            assertTrue("evictor must report running after start()", evictor.isRunning());
            evictor.close();
            assertFalse("evictor must not report running after close()", evictor.isRunning());
        } finally {
            evictor.close();
        }
    }

    public void testCloseCancelsScheduledTask() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingConnectionManager manager = new RecordingConnectionManager();

        final var evictor = new HttpConnectionEvictor(taskQueue.getThreadPool(), manager, INTERVAL, MAX_IDLE);
        evictor.start();
        evictor.close();

        if (taskQueue.hasDeferredTasks()) {
            taskQueue.advanceTime();
        }
        taskQueue.runAllRunnableTasks();

        assertEquals("close() must prevent any further eviction passes from running", 0, manager.closeExpiredCalls.get());
        assertFalse("close() must drain the deferred eviction task", taskQueue.hasDeferredTasks());
    }

    public void testEvictionFailureIsSwallowedAndRescheduled() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingConnectionManager manager = new RecordingConnectionManager();
        manager.closeExpiredAction = () -> { throw new IllegalStateException("simulated transient failure"); };

        try (var evictor = new HttpConnectionEvictor(taskQueue.getThreadPool(), manager, INTERVAL, MAX_IDLE)) {
            evictor.start();

            advanceOneInterval(taskQueue);
            assertEquals(1, manager.closeExpiredCalls.get());

            advanceOneInterval(taskQueue);
            assertEquals("evictor must continue running after a failed pass", 2, manager.closeExpiredCalls.get());
        }
    }

    private static void advanceOneInterval(DeterministicTaskQueue taskQueue) {
        assertTrue("expected a deferred eviction task", taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    /**
     * Minimal {@link NHttpClientConnectionManager} stub that records invocations of the two eviction
     * methods the evictor relies on. All other interface methods throw {@link UnsupportedOperationException}
     * so that any unexpected interaction surfaces immediately rather than passing silently.
     */
    private static final class RecordingConnectionManager implements NHttpClientConnectionManager {

        final AtomicInteger closeExpiredCalls = new AtomicInteger();
        final AtomicInteger closeIdleCalls = new AtomicInteger();
        final AtomicLong lastIdleMillis = new AtomicLong(-1L);
        Runnable closeExpiredAction = () -> {};

        @Override
        public void closeExpiredConnections() {
            closeExpiredCalls.incrementAndGet();
            closeExpiredAction.run();
        }

        @Override
        public void closeIdleConnections(long idletime, TimeUnit timeUnit) {
            closeIdleCalls.incrementAndGet();
            lastIdleMillis.set(timeUnit.toMillis(idletime));
        }

        @Override
        public Future<NHttpClientConnection> requestConnection(
            HttpRoute route,
            Object state,
            long connectTimeout,
            long connectionRequestTimeout,
            TimeUnit timeUnit,
            FutureCallback<NHttpClientConnection> callback
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void releaseConnection(NHttpClientConnection conn, Object newState, long validDuration, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startRoute(NHttpClientConnection conn, HttpRoute route, HttpContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void upgrade(NHttpClientConnection conn, HttpRoute route, HttpContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void routeComplete(NHttpClientConnection conn, HttpRoute route, HttpContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRouteComplete(NHttpClientConnection conn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(IOEventDispatch eventDispatch) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
