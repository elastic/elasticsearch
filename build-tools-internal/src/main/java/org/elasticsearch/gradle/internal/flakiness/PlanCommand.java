/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.util.List;

/**
 * One ready-to-run batch command carried in {@code flakiness-plan.json} (the {@code commands} array). The
 * Java resolver now owns batch-command generation (batching, iteration counts, the repeat-rest wrapper), so
 * the TypeScript {@code generate} step is a thin consumer that only maps these to Buildkite steps.
 *
 * <p><b>Target neutrality:</b> {@link #command} contains the literal token {@value #GRADLE_PLACEHOLDER}
 * wherever the gradle binary belongs (both plain invocations and inside the
 * {@code repeat-rest-test.sh &lt;iters&gt; __GRADLE__ &lt;tasks&gt;} form). The thin runner layer replaces it
 * with the target-appropriate wrapper ({@code .ci/scripts/run-gradle.sh} on CI, {@code ./gradlew} locally),
 * so the plan itself is not tied to either environment.
 *
 * @param kind      the {@link Kinds} test kind
 * @param label     human label for the Buildkite step
 * @param key       Buildkite step key ({@code flakiness-detection:...})
 * @param command   the gradle invocation with the {@value #GRADLE_PLACEHOLDER} placeholder for the binary
 * @param taskPaths the distinct {@code Test}-task paths this command invokes, carried <em>alongside</em> the
 *                  command rather than parsed back out of it. The batch runner needs them to answer "did the
 *                  task I asked for actually run?": Gradle reports an {@code onlyIf}-rejected task as
 *                  {@code SKIPPED} with zero tests and exit 0, which is otherwise indistinguishable from a
 *                  hang. A build's task status also contains unrelated {@code SKIPPED} entries (a
 *                  {@code processResources} with no resources, say), so the check must be scoped to exactly
 *                  these paths - which is why they are a field and not a regex over {@link #command}
 */
public record PlanCommand(String kind, String label, String key, String command, List<String> taskPaths) {

    /** The placeholder the runner layer replaces with the target-appropriate gradle binary. */
    public static final String GRADLE_PLACEHOLDER = "__GRADLE__";
}
