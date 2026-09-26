/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Watches for GCP Spot VM preemption and raises a process-global flag when the VM is
 * scheduled for termination.
 *
 * <p>Two detection modes, selected at startup:
 * <ul>
 *   <li><b>Simulation</b> ({@code GCP_PREEMPTION_SIMULATE_AFTER_SECONDS=N}): fires after N
 *       seconds. Use this for local testing — no GCP required.</li>
 *   <li><b>Real</b>: short-polls the GCP instance metadata server every
 *       {@value #POLL_INTERVAL_SECONDS}s, checking whether the {@code preempted} flag has
 *       flipped to {@code TRUE}.</li>
 * </ul>
 *
 * <p>Activated by setting {@code GCP_PREEMPTION_WATCHDOG=true}. No-op otherwise.
 */
public final class GcpPreemptionWatchdog {

    private static final String METADATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted";

    private static final int POLL_INTERVAL_SECONDS = 1;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);

    private static final String ENV_WATCHDOG = "GCP_PREEMPTION_WATCHDOG";
    private static final String ENV_SIMULATE = "GCP_PREEMPTION_SIMULATE_AFTER_SECONDS";

    private static final AtomicBoolean PREEMPTED = new AtomicBoolean(false);
    private static final AtomicReference<Instant> PREEMPTED_AT = new AtomicReference<>();
    private static final List<Runnable> LISTENERS = new CopyOnWriteArrayList<>();
    private static volatile Thread watchThread;

    private GcpPreemptionWatchdog() {}

    public static boolean isPreempted() {
        return PREEMPTED.get();
    }

    public static Instant preemptedAt() {
        return PREEMPTED_AT.get();
    }

    /**
     * Register a callback to fire when preemption is first detected. Runs on the watchdog
     * thread, so listeners must be non-blocking and self-contained. If preemption has already
     * been signalled, the listener fires immediately on the caller's thread instead.
     */
    public static void onPreempted(Runnable listener) {
        LISTENERS.add(listener);
        if (PREEMPTED.get()) {
            safelyRun(listener);
        }
    }

    public static synchronized void start() {
        if (watchThread != null && watchThread.isAlive()) {
            return;
        }
        if (Boolean.parseBoolean(System.getenv(ENV_WATCHDOG)) == false) {
            return;
        }

        String simulateAfter = System.getenv(ENV_SIMULATE);
        if (simulateAfter != null && simulateAfter.isEmpty() == false) {
            long seconds;
            try {
                seconds = Long.parseLong(simulateAfter);
            } catch (NumberFormatException e) {
                log("invalid " + ENV_SIMULATE + "=" + simulateAfter + "; ignoring simulation");
                seconds = -1;
            }
            if (seconds > 0) {
                long finalSeconds = seconds;
                watchThread = new Thread(() -> simulationLoop(finalSeconds), "gcp-preemption-watchdog");
                watchThread.setDaemon(true);
                watchThread.start();
                log("simulation mode: will fire in " + seconds + "s");
                return;
            }
        }

        watchThread = new Thread(GcpPreemptionWatchdog::metadataLoop, "gcp-preemption-watchdog");
        watchThread.setDaemon(true);
        watchThread.start();
        log("started; polling " + METADATA_BASE_URL + " every " + POLL_INTERVAL_SECONDS + "s");
    }

    public static synchronized void stop() {
        Thread t = watchThread;
        if (t == null) {
            return;
        }
        watchThread = null;
        t.interrupt();
        try {
            t.join(Duration.ofSeconds(2).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void simulationLoop(long delaySeconds) {
        try {
            Thread.sleep(Duration.ofSeconds(delaySeconds).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        signal("simulated after " + delaySeconds + "s (set via " + ENV_SIMULATE + ")");
    }

    private static void metadataLoop() {
        HttpClient client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(METADATA_BASE_URL))
            .header("Metadata-Flavor", "Google")
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .build();

        boolean loggedFailure = false;
        while (Thread.currentThread().isInterrupted() == false) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                loggedFailure = false;
                if (response.statusCode() == 200 && "TRUE".equalsIgnoreCase(response.body().trim())) {
                    signal("GCP metadata server reported preempted=TRUE");
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (loggedFailure == false) {
                    log("metadata poll failed (will retry silently): " + e);
                    loggedFailure = true;
                }
            }
            try {
                Thread.sleep(Duration.ofSeconds(POLL_INTERVAL_SECONDS).toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void signal(String reason) {
        if (PREEMPTED.compareAndSet(false, true)) {
            PREEMPTED_AT.set(Instant.now());
            log("preemption detected at " + PREEMPTED_AT.get() + ": " + reason);
            for (Runnable listener : LISTENERS) {
                safelyRun(listener);
            }
        }
    }

    private static void safelyRun(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            System.err.println("[gcp-preemption-watchdog] listener threw: " + t);
            t.printStackTrace(System.err);
        }
    }

    private static void log(String message) {
        System.out.println("[gcp-preemption-watchdog] " + message);
    }
}
