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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static final String MARKER_FILENAME = ".preemption-marker.json";

    private final CancellationTokenSource tokenSource;
    private final File projectDir;
    private volatile boolean cancelled;

    public BuildCanceller(CancellationTokenSource tokenSource, File projectDir) {
        this.tokenSource = tokenSource;
        this.projectDir = projectDir;
    }

    /**
     * Registers this canceller as a listener on the preemption watchdog. When preemption
     * is detected, the build is cancelled, a marker file is written for the build scan
     * script to read, and descendant worker processes are killed.
     */
    public void install() {
        GcpPreemptionWatchdog.onPreempted(() -> {
            cancelled = true;
            writeMarkerFile();
            writePreemptionExitFile();
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
     * Writes a JSON marker file into the project's build directory so the Gradle build scan
     * script can detect that preemption occurred and tag the scan accordingly. Written before
     * the cancellation token fires so the file is available when {@code buildFinished} runs.
     */
    private void writeMarkerFile() {
        try {
            File buildDir = new File(projectDir, "build");
            buildDir.mkdirs();
            File marker = new File(buildDir, MARKER_FILENAME);
            try (PrintWriter w = new PrintWriter(marker, "UTF-8")) {
                w.printf("{ \"preempted\": true, \"preemptedAt\": \"%s\" }%n", GcpPreemptionWatchdog.preemptedAt());
            }
        } catch (IOException e) {
            System.err.println("[gcp-preemption-watchdog] failed to write marker file: " + e.getMessage());
        }
    }

    /**
     * Writes the preemption exit code to {@code /tmp/gradle-preemption-exit-<jobId>} so that
     * the Buildkite {@code post-command} hook can re-exit with it. Written immediately on
     * preemption detection so the file exists even if the VM is killed before the build finishes.
     */
    private static void writePreemptionExitFile() {
        String envCode = System.getenv("GCP_PREEMPTION_EXIT_CODE");
        int exitCode = 47;
        if (envCode != null) {
            try {
                exitCode = Integer.parseInt(envCode);
            } catch (NumberFormatException e) {
                // fall through to default
            }
        }
        String jobId = System.getenv("BUILDKITE_JOB_ID");
        Path exitFile = Path.of("/tmp", "gradle-preemption-exit-" + (jobId != null ? jobId : "local"));
        try {
            Files.writeString(exitFile, Integer.toString(exitCode));
            System.out.println("[gcp-preemption-watchdog] preemption exit code written to " + exitFile);
        } catch (IOException e) {
            System.err.println("[gcp-preemption-watchdog] failed to write preemption exit file: " + e.getMessage());
        }
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
