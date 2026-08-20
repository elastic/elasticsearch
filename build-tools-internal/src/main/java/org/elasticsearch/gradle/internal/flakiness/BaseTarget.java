/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * A resolved base target: the project/sourceSet/kind a {@link FlakinessRef} was resolved to, before
 * bytecode enrichment. It is fully authoritative - every field is derived from the owning project's real
 * configured model (captured by {@link FlakinessProjectResolvePlugin}), including the exact {@code compileTaskPath} the
 * compile step must run, the {@code outputDir} the scan step must scan, and the {@code runnableTasks} that
 * actually re-run this target.
 *
 * <p>A base target may still be abstract (in which case {@link PlanBuilder} flattens it into concrete
 * subclasses). yaml suite/runner targets carry a {@code suitePath} rather than an {@code fqcn}; a
 * parameterised yaml case carries both {@code fqcn} and {@code yamlTest}.
 *
 * <p>{@code runnableTasks} / {@code skipReason} are the resolved <em>disposition</em>, computed by
 * {@link TestTaskSelector} from the project's real {@code Test} tasks. They replace the old
 * {@code bwc}-marker heuristic: instead of "this project applies {@code elasticsearch.bwc-test}, so give up",
 * the resolver now names the tasks that genuinely run the target (making bwc tests re-runnable), and only
 * reports {@code skipReason} when the model or the agent's capabilities really leave nothing to run.
 *
 * <p>Serialized into each project's {@code <project>.json} (the resolve-&gt;scan hand-off), so it must
 * round-trip through Jackson.
 *
 * @param gradleProject   owning Gradle project path
 * @param sourceSet       owning source-set name
 * @param kind            wire kind (see {@link Kinds})
 * @param fqcn            fully-qualified class name, or {@code null} for yaml suite/runner targets
 * @param suitePath       yaml suite path, or {@code null}
 * @param yamlTest        parameterised yaml case descriptor, or {@code null}
 * @param compileTaskPath authoritative {@code compile&lt;Ss&gt;Java} task path for this target's source set
 * @param outputDir       authoritative compiled-classes output directory for this target's source set
 * @param runnableTasks   the task paths that actually run this target, capped and newest-first; empty when
 *                        {@code skipReason} is set
 * @param candidateTasks  how many enabled candidate tasks existed before the cap (for the plan's report)
 * @param skipReason      {@code null} when runnable, else why not (see {@link TestTaskSelector})
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BaseTarget(
    String gradleProject,
    String sourceSet,
    String kind,
    String fqcn,
    String suitePath,
    String yamlTest,
    String compileTaskPath,
    String outputDir,
    List<String> runnableTasks,
    int candidateTasks,
    String skipReason
) {

    /** Whether this target has at least one task that can re-run it on this agent. */
    public boolean runnable() {
        return skipReason == null;
    }
}
