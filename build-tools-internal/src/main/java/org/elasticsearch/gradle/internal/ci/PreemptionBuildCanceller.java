/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.Task;
import org.gradle.api.internal.GradleInternal;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.initialization.BuildCancellationToken;

import java.util.List;

/**
 * Reacts to a {@link GcpPreemptionWatchdog} signal by stopping the Gradle build immediately.
 *
 * <p>Four layers, each covering a different category of in-flight work:
 * <ol>
 *   <li><b>{@code onlyIf}</b> on every task: anything not yet started is reported SKIPPED
 *       rather than executed.</li>
 *   <li><b>{@link BuildCancellationToken}</b>: tells Gradle's task-graph executor to stop
 *       dispatching new work (same signal as Ctrl+C).</li>
 *   <li><b>Force-kill all descendant processes</b>: every child JVM spawned by the build
 *       (test workers, compiler daemons, other workers) is destroyed immediately via
 *       {@link ProcessHandle#destroyForcibly()}. This is what achieves actual immediacy —
 *       the cancellation token alone is cooperative and waits for the current operation to
 *       finish.</li>
 *   <li><b>Interrupt Gradle task-execution threads</b>: covers tasks that run inside the
 *       Gradle daemon's own JVM rather than in a forked process (e.g. in-JVM compilation
 *       when no compiler daemon is active). Threads whose name starts with
 *       {@code "Execution worker"} are interrupted; Gradle's finalization threads are
 *       left untouched so the build scan can still publish.</li>
 * </ol>
 *
 * <p>In addition, a {@code doFirst} action is prepended to every task at {@code whenReady}
 * time. If preemption has fired by the time a task begins executing, the action throws
 * {@link StopExecutionException}, which Gradle treats as a clean completion (no failure in
 * the build-operation result). This prevents tasks that start after preemption from
 * appearing as red/failed in the Develocity build scan. Tasks that were already running
 * when preemption fired are stopped via force-kill and thread interruption; those will
 * still show as failed in the scan, though the PREEMPTED tag marks them as non-representative.
 */
public final class PreemptionBuildCanceller {

    private static final Logger LOGGER = Logging.getLogger(PreemptionBuildCanceller.class);

    private PreemptionBuildCanceller() {}

    public static void install(Gradle gradle) {
        gradle.getTaskGraph().whenReady(graph -> {
            for (Task task : graph.getAllTasks()) {
                task.onlyIf("not preempted by GCP", t -> GcpPreemptionWatchdog.isPreempted() == false);
                task.doFirst("stop cleanly if preempted", t -> {
                    if (GcpPreemptionWatchdog.isPreempted()) {
                        throw new StopExecutionException("preempted by GCP");
                    }
                });
                task.doLast("stop cleanly if preempted", t -> {
                    if (GcpPreemptionWatchdog.isPreempted()) {
                        throw new StopExecutionException("preempted by GCP");
                    }
                });
            }
        });

        GcpPreemptionWatchdog.onPreempted(() -> {
            cancelBuild(gradle);
            killDescendantProcesses();
            interruptTaskExecutionThreads();
        });
    }

    private static void cancelBuild(Gradle gradle) {
        try {
            BuildCancellationToken token = ((GradleInternal) gradle).getServices().get(BuildCancellationToken.class);
            LOGGER.lifecycle("[gcp-preemption-watchdog] cancelling Gradle build via BuildCancellationToken");
            token.cancel();
        } catch (Throwable t) {
            LOGGER.warn(
                "[gcp-preemption-watchdog] could not resolve BuildCancellationToken; "
                    + "pending tasks will still be skipped via onlyIf, but running tasks will not be interrupted",
                t
            );
        }
    }

    private static void killDescendantProcesses() {
        List<ProcessHandle> descendants = ProcessHandle.current().descendants().toList();
        if (descendants.isEmpty()) {
            LOGGER.lifecycle("[gcp-preemption-watchdog] no descendant processes found");
        } else {
            LOGGER.lifecycle("[gcp-preemption-watchdog] force-killing {} descendant process(es)", descendants.size());
            for (ProcessHandle p : descendants) {
                p.destroyForcibly();
            }
        }
    }

    /**
     * Interrupts threads that are running Gradle task actions inside the build JVM. Gradle
     * names these threads {@code "Execution worker Thread N"} in its parallel executor.
     * Interrupting them causes in-flight task actions (e.g. in-JVM javac) to bail out.
     *
     * <p>Threads are identified by name prefix so we don't accidentally interrupt Gradle's
     * own lifecycle threads (build scan publisher, shutdown hooks, etc.).
     */
    private static void interruptTaskExecutionThreads() {
        int interrupted = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("Execution worker")) {
                t.interrupt();
                interrupted++;
            }
        }
        if (interrupted > 0) {
            LOGGER.lifecycle("[gcp-preemption-watchdog] interrupted {} task-execution thread(s)", interrupted);
        }
    }

}
