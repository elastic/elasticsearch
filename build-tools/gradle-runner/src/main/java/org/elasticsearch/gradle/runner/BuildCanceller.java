/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.gradle.tooling.CancellationTokenSource;

import java.util.List;

/**
 * Reacts to a {@link GcpPreemptionWatchdog} signal by cancelling the Gradle build via the
 * Tooling API's {@link CancellationTokenSource} and force-killing all descendant processes.
 *
 * <p>Two layers:
 * <ol>
 *   <li><b>{@link CancellationTokenSource#cancel()}</b>: tells Gradle's task executor to
 *       stop dispatching new work (equivalent to Ctrl+C from the CLI).</li>
 *   <li><b>Force-kill all descendant processes</b>: every child JVM spawned by the build
 *       (test workers, compiler daemons) is destroyed immediately via
 *       {@link ProcessHandle#destroyForcibly()}.</li>
 * </ol>
 */
public final class BuildCanceller {

    private final CancellationTokenSource tokenSource;
    private volatile boolean cancelled;

    public BuildCanceller(CancellationTokenSource tokenSource) {
        this.tokenSource = tokenSource;
    }

    /**
     * Registers this canceller as a listener on the preemption watchdog. When preemption
     * is detected, the build is cancelled and descendant processes are killed.
     */
    public void install() {
        GcpPreemptionWatchdog.onPreempted(() -> {
            cancelled = true;
            cancelBuild();
            killDescendantProcesses();
        });
    }

    public boolean isCancelled() {
        return cancelled;
    }

    private void cancelBuild() {
        System.out.println("[gcp-preemption-watchdog] cancelling Gradle build via CancellationToken");
        tokenSource.cancel();
    }

    /**
     * Force-kills worker processes spawned by the Gradle daemon (test workers, compiler
     * daemons) without killing the daemon itself. The daemon must stay alive so it can
     * process the cancellation token, finalize the build scan, and shut down cleanly.
     *
     * <p>Our direct children are Gradle daemon processes; their children are the workers
     * we want to kill.
     */
    private static void killDescendantProcesses() {
        List<ProcessHandle> workers = ProcessHandle.current().children().flatMap(ProcessHandle::children).toList();
        if (workers.isEmpty()) {
            System.out.println("[gcp-preemption-watchdog] no worker processes found");
        } else {
            System.out.println("[gcp-preemption-watchdog] force-killing " + workers.size() + " worker process(es)");
            for (ProcessHandle p : workers) {
                p.destroyForcibly();
            }
        }
    }
}
