/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches for GCP Spot VM preemption and raises a process-global flag when the VM is
 * scheduled for termination.
 *
 * <p>Two detection modes, selected at startup:
 * <ul>
 *   <li><b>Simulation</b> ({@code GCP_PREEMPTION_SIMULATE_AFTER_SECONDS=N}): fires after N
 *       seconds. Use this for local testing — no GCP required.</li>
 *   <li><b>Real</b>: long-polls the GCP instance metadata server using the
 *       {@code wait_for_change} parameter so the server holds the connection open until the
 *       {@code preempted} flag flips. Single blocking request per poll cycle; much lower
 *       overhead than short-interval polling and responds within ~1s of the actual signal.</li>
 * </ul>
 *
 * <p>Activated by setting {@code GCP_PREEMPTION_WATCHDOG=true}. No-op otherwise.
 *
 * <p>State is intentionally static: build-script reapplication and Gradle daemon reuse mean
 * we want a single watchdog per JVM, idempotent {@link #start()}/{@link #stop()}, and a flag
 * any part of the build can read without plumbing a service through configuration.
 */
public final class GcpPreemptionWatchdog {

    private static final String METADATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted";

    // Long-poll: ask the metadata server to hold the connection until the value changes or
    // the timeout elapses. This gives sub-second detection latency with a single open socket
    // rather than a 1-second polling loop.
    private static final int LONG_POLL_TIMEOUT_SECONDS = 60;
    private static final String METADATA_LONG_POLL_URL = METADATA_BASE_URL
        + "?wait_for_change=true&timeout_sec="
        + LONG_POLL_TIMEOUT_SECONDS;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(LONG_POLL_TIMEOUT_SECONDS + 15);

    private static final String ENV_WATCHDOG = "GCP_PREEMPTION_WATCHDOG";
    private static final String ENV_SIMULATE = "GCP_PREEMPTION_SIMULATE_AFTER_SECONDS";

    private static final Logger LOGGER = Logging.getLogger(GcpPreemptionWatchdog.class);

    private static final AtomicBoolean PREEMPTED = new AtomicBoolean(false);
    private static final List<Runnable> LISTENERS = new CopyOnWriteArrayList<>();
    private static volatile Thread watchThread;

    private GcpPreemptionWatchdog() {}

    public static boolean isPreempted() {
        return PREEMPTED.get();
    }

    /**
     * Register a callback to fire when preemption is first detected. Runs on the watchdog
     * thread, so listeners must be non-blocking and self-contained (no Gradle DSL calls).
     * If preemption has already been signalled, the listener fires immediately on the
     * caller's thread instead.
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
                LOGGER.warn("[gcp-preemption-watchdog] invalid {}={}; ignoring simulation", ENV_SIMULATE, simulateAfter);
                seconds = -1;
            }
            if (seconds > 0) {
                long finalSeconds = seconds;
                watchThread = new Thread(() -> simulationLoop(finalSeconds), "gcp-preemption-watchdog");
                watchThread.setDaemon(true);
                watchThread.start();
                LOGGER.lifecycle("[gcp-preemption-watchdog] simulation mode: will fire in {}s", seconds);
                return;
            }
        }

        watchThread = new Thread(GcpPreemptionWatchdog::metadataLoop, "gcp-preemption-watchdog");
        watchThread.setDaemon(true);
        watchThread.start();
        LOGGER.lifecycle("[gcp-preemption-watchdog] started; long-polling {}", METADATA_LONG_POLL_URL);
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
        HttpRequest request = HttpRequest.newBuilder(URI.create(METADATA_LONG_POLL_URL))
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
                // Server returned FALSE after its hold period (timeout_sec elapsed without
                // the value changing). Loop and reconnect — still not preempted.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (loggedFailure == false) {
                    LOGGER.info("[gcp-preemption-watchdog] metadata poll failed (will retry silently): {}", e.toString());
                    loggedFailure = true;
                } else {
                    LOGGER.debug("[gcp-preemption-watchdog] metadata poll failed: {}", e.toString());
                }
                // Back off briefly before retrying so we don't spin on a non-GCP host.
                try {
                    Thread.sleep(Duration.ofSeconds(10).toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static void signal(String reason) {
        if (PREEMPTED.compareAndSet(false, true)) {
            LOGGER.lifecycle("[gcp-preemption-watchdog] preemption detected: {}", reason);
            for (Runnable listener : LISTENERS) {
                safelyRun(listener);
            }
        }
    }

    private static void safelyRun(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOGGER.warn("[gcp-preemption-watchdog] listener threw", t);
        }
    }
}
