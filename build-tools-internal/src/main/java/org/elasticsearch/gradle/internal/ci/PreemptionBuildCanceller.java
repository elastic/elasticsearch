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
import org.gradle.initialization.BuildCancellationToken;

/**
 * Wires {@link GcpPreemptionWatchdog} into the Gradle task graph so a preemption signal
 * actually stops work.
 *
 * <p>Two layers, because Gradle has no single mechanism that covers both pending and
 * running tasks:
 * <ul>
 *   <li><b>Pending tasks</b>: an {@code onlyIf} predicate is attached to every task at
 *       graph-ready time. When preemption fires, the predicate flips and any task that has
 *       not yet started is reported as SKIPPED rather than executed.</li>
 *   <li><b>Running tasks</b>: Gradle's internal {@link BuildCancellationToken} is signalled,
 *       which is the same mechanism Ctrl+C and the Tooling API use. Tasks that cooperate
 *       with cancellation (notably {@code Test}) bail at their next checkpoint; the graph
 *       executor stops dispatching new work.</li>
 * </ul>
 *
 * <p>The {@code BuildCancellationToken} access goes through {@link GradleInternal}, which
 * is technically internal API. We catch failures and degrade to the {@code onlyIf} layer
 * if the cast or service lookup ever breaks across a Gradle upgrade.
 */
public final class PreemptionBuildCanceller {

    private static final Logger LOGGER = Logging.getLogger(PreemptionBuildCanceller.class);

    private PreemptionBuildCanceller() {}

    public static void install(Gradle gradle) {
        gradle.getTaskGraph().whenReady(graph -> {
            for (Task task : graph.getAllTasks()) {
                task.onlyIf("not preempted by GCP", t -> GcpPreemptionWatchdog.isPreempted() == false);
            }
        });

        GcpPreemptionWatchdog.onPreempted(() -> {
            BuildCancellationToken token;
            try {
                token = ((GradleInternal) gradle).getServices().get(BuildCancellationToken.class);
            } catch (Throwable t) {
                LOGGER.warn(
                    "[gcp-preemption-watchdog] could not resolve BuildCancellationToken; "
                        + "pending tasks will still be skipped via onlyIf, but running tasks will not be interrupted",
                    t
                );
                return;
            }
            LOGGER.lifecycle("[gcp-preemption-watchdog] cancelling Gradle build via BuildCancellationToken");
            token.cancel();
        });
    }
}
