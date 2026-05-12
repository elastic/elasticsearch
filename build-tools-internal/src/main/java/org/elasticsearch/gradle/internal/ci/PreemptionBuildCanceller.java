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
 * Reacts to a {@link GcpPreemptionWatchdog} signal by stopping the Gradle build.
 *
 * <ul>
 *   <li>{@code onlyIf} on every task: anything that hasn't started yet is reported as SKIPPED.</li>
 *   <li>{@link BuildCancellationToken}: same path as Ctrl+C — stops dispatching new test cases
 *       to forks and lets currently-running tests finish cooperatively. Tests that complete
 *       before the token fires keep their real result; tests in-flight when workers are stopped
 *       appear however Gradle naturally reports them.</li>
 * </ul>
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
        });
    }
}
