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
 * Polls the GCP instance metadata server for a Spot VM preemption notice and raises a
 * process-global flag when the VM is scheduled for termination. The flag is the input signal
 * for the rest of the preemption-handling pipeline (task graph cancellation, test-result
 * rewriting, build scan publish, custom exit code).
 *
 * <p>GCP gives Spot VMs roughly 30 seconds between the {@code preempted} metadata flag
 * flipping to {@code TRUE} and the underlying VM being killed, so polling cadence must be
 * fast (sub-second budget per poll) and listeners must not block the poll loop.
 *
 * <p>Activated by setting the {@code GCP_PREEMPTION_WATCHDOG} env var to {@code true}.
 * No-op otherwise so local and non-GCP CI builds incur no overhead.
 *
 * <p>State is intentionally static: build-script reapplication and Gradle daemon reuse mean
 * we want a single watchdog per JVM, idempotent {@link #start()}/{@link #stop()}, and a flag
 * any part of the build can read without plumbing a service through configuration.
 */
public final class GcpPreemptionWatchdog {

    private static final String METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted";
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final String ENV_FLAG = "GCP_PREEMPTION_WATCHDOG";

    private static final Logger LOGGER = Logging.getLogger(GcpPreemptionWatchdog.class);

    private static final AtomicBoolean PREEMPTED = new AtomicBoolean(false);
    private static final List<Runnable> LISTENERS = new CopyOnWriteArrayList<>();
    private static volatile Thread pollThread;

    private GcpPreemptionWatchdog() {}

    public static boolean isPreempted() {
        return PREEMPTED.get();
    }

    /**
     * Register a callback to fire when preemption is first detected. Runs on the watchdog
     * thread, so listeners must be non-blocking and self-contained (no Gradle DSL calls).
     * If preemption has already been signalled, the listener fires immediately on the
     * caller's thread.
     */
    public static void onPreempted(Runnable listener) {
        LISTENERS.add(listener);
        if (PREEMPTED.get()) {
            safelyRun(listener);
        }
    }

    public static synchronized void start() {
        if (pollThread != null && pollThread.isAlive()) {
            return;
        }
        if (Boolean.parseBoolean(System.getenv(ENV_FLAG)) == false) {
            return;
        }
        Thread t = new Thread(GcpPreemptionWatchdog::pollLoop, "gcp-preemption-watchdog");
        t.setDaemon(true);
        t.start();
        pollThread = t;
        LOGGER.lifecycle("[gcp-preemption-watchdog] started; polling {} every {}s", METADATA_URL, POLL_INTERVAL.toSeconds());
    }

    public static synchronized void stop() {
        Thread t = pollThread;
        if (t == null) {
            return;
        }
        pollThread = null;
        t.interrupt();
        try {
            t.join(Duration.ofSeconds(2).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void pollLoop() {
        HttpClient client = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(METADATA_URL))
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
                    if (PREEMPTED.compareAndSet(false, true)) {
                        LOGGER.lifecycle("[gcp-preemption-watchdog] preemption signal received from GCP metadata server");
                        for (Runnable listener : LISTENERS) {
                            safelyRun(listener);
                        }
                    }
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Log the first failure at info (likely a misconfiguration if the env var is set on a non-GCP host);
                // subsequent failures at debug to avoid spamming.
                if (loggedFailure == false) {
                    LOGGER.info("[gcp-preemption-watchdog] metadata poll failed (will keep retrying silently): {}", e.toString());
                    loggedFailure = true;
                } else {
                    LOGGER.debug("[gcp-preemption-watchdog] metadata poll failed: {}", e.toString());
                }
            }

            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
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
