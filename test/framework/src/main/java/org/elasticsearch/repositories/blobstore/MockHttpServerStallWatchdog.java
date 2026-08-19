/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Warns when a mock HTTP server worker gets stuck reading a request that never finishes arriving.
 *
 * <p>{@code com.sun.net.httpserver} hands a connection to a worker as soon as it becomes readable and then parses the
 * request head on that worker. If the head never completes, the worker blocks in {@code sun.net.httpserver} indefinitely
 * — and because the server will not dispatch a second exchange for a connection that already has one in flight, that
 * connection is dead until something else intervenes. With the low worker count these fixtures use, a couple of such
 * requests make the whole fixture unresponsive. That is what failed
 * {@code GoogleCloudStorageBlobStoreRepositoryTests#testSnapshotWithLargeSegmentFiles}
 * (<a href="https://github.com/elastic/elasticsearch/issues/156468">156468</a>).
 *
 * <p>{@code sun.net.httpserver.maxReqTime} (set for test JVMs by the build) now bounds that wait, so the server closes
 * such a request instead of stalling until the client's own read timeout fires. The tests therefore pass — which means
 * the condition would otherwise become invisible. It is not yet known what leaves a request incomplete, and the
 * candidates include the client rather than the fixture, so this watchdog keeps the occurrences observable: it logs a
 * warning and does <em>not</em> fail anything.
 *
 * <p>Whether a worker is "stuck" is decided from {@link #requestStarted}/{@link #requestFinished} bookkeeping rather
 * than by matching package names on the stack. The executor that submits exchanges is declared in this package, so a
 * package-prefix check matches every worker and never fires.
 */
class MockHttpServerStallWatchdog {

    private static final Logger logger = LogManager.getLogger(MockHttpServerStallWatchdog.class);

    /**
     * Report a worker stuck for longer than this. Comfortably longer than any legitimate request to an in-process mock
     * server, and shorter than {@code sun.net.httpserver.maxReqTime} so the evidence is captured before the server
     * closes the request out from under us.
     */
    private static final long STALL_WARN_THRESHOLD_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long SAMPLE_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(2);

    private static final String JDK_HTTP_SERVER_PACKAGE = "sun.net.httpserver";

    /** Requests currently being handled, keyed by worker thread id. */
    private final Map<Long, String> inFlightRequests = new ConcurrentHashMap<>();
    /** The last request each worker completed, which is the best clue to what preceded a stall on that connection. */
    private final Map<Long, String> lastCompletedRequests = new ConcurrentHashMap<>();

    private final String workerThreadNameMarker;
    private volatile Thread watchdogThread;

    MockHttpServerStallWatchdog(String workerThreadNameMarker) {
        this.workerThreadNameMarker = workerThreadNameMarker;
    }

    void requestStarted(String request) {
        inFlightRequests.put(Thread.currentThread().threadId(), request);
    }

    void requestFinished(String request) {
        final long threadId = Thread.currentThread().threadId();
        inFlightRequests.remove(threadId);
        lastCompletedRequests.put(threadId, request);
    }

    synchronized void start() {
        if (watchdogThread != null) {
            return;
        }
        final Thread thread = new Thread(this::run, "mock-http-server-stall-watchdog");
        thread.setDaemon(true);
        watchdogThread = thread;
        thread.start();
    }

    synchronized void stop() {
        final Thread thread = watchdogThread;
        watchdogThread = null;
        if (thread != null) {
            thread.interrupt();
        }
        inFlightRequests.clear();
        lastCompletedRequests.clear();
    }

    private void run() {
        // Per-thread time at which the current stall was first seen, so a worker that is merely busy is not reported.
        final Map<Long, Long> stalledSince = new HashMap<>();
        final Set<Long> alreadyReported = new HashSet<>();
        while (watchdogThread == Thread.currentThread()) {
            try {
                TimeUnit.MILLISECONDS.sleep(SAMPLE_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                return;
            }
            try {
                sample(stalledSince, alreadyReported);
            } catch (Exception e) {
                logger.warn("mock http server stall watchdog failed", e);
            }
        }
    }

    private void sample(Map<Long, Long> stalledSince, Set<Long> alreadyReported) {
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        // Names first, with no stack traces, so the common case costs almost nothing: capturing stacks for every thread
        // in an integration test JVM on every tick would be far too expensive for a diagnostic that rarely has anything
        // to say. Thread#getAllStackTraces is a forbidden API here in any case.
        final List<Long> workerIds = new ArrayList<>();
        for (ThreadInfo info : threadBean.getThreadInfo(threadBean.getAllThreadIds(), 0)) {
            if (info != null && info.getThreadName().contains(workerThreadNameMarker)) {
                workerIds.add(info.getThreadId());
            }
        }

        stalledSince.keySet().retainAll(workerIds);
        alreadyReported.retainAll(workerIds);

        // A worker handling a request is busy, not stuck, so it needs no stack.
        workerIds.removeIf(inFlightRequests::containsKey);
        if (workerIds.isEmpty()) {
            return;
        }

        final long now = System.currentTimeMillis();
        final long[] idsNeedingStacks = workerIds.stream().mapToLong(Long::longValue).toArray();
        for (ThreadInfo info : threadBean.getThreadInfo(idsNeedingStacks, Integer.MAX_VALUE)) {
            if (info == null) {
                continue;
            }
            final long threadId = info.getThreadId();
            if (isInsideJdkHttpServer(info) == false) {
                // Idle in the executor's queue rather than parsing a request.
                stalledSince.remove(threadId);
                alreadyReported.remove(threadId);
                continue;
            }
            final long since = stalledSince.computeIfAbsent(threadId, id -> now);
            final long stalledForMillis = now - since;
            if (stalledForMillis >= STALL_WARN_THRESHOLD_MILLIS && alreadyReported.add(threadId)) {
                warn(info, stalledForMillis);
            }
        }
    }

    /** A worker inside the JDK http server but not inside a handler is parsing a request that has not fully arrived. */
    private static boolean isInsideJdkHttpServer(ThreadInfo info) {
        for (StackTraceElement frame : info.getStackTrace()) {
            if (frame.getClassName().startsWith(JDK_HTTP_SERVER_PACKAGE)) {
                return true;
            }
        }
        return false;
    }

    private void warn(ThreadInfo info, long stalledForMillis) {
        final StringBuilder message = new StringBuilder();
        message.append("mock http server worker [")
            .append(info.getThreadName())
            .append("] has been reading an incomplete request for [")
            .append(stalledForMillis)
            .append("ms]; this connection cannot serve anything until sun.net.httpserver.maxReqTime closes it. ")
            .append("Last request completed by this worker was [")
            .append(lastCompletedRequests.getOrDefault(info.getThreadId(), "none"))
            .append("]. Other workers are handling ")
            .append(inFlightRequests.values())
            .append(". See https://github.com/elastic/elasticsearch/issues/156468\n");
        for (StackTraceElement frame : info.getStackTrace()) {
            message.append("\tat ").append(frame).append('\n');
        }
        logger.warn(message.toString());
    }
}
