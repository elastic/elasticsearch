/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * The {@code flakiness-plan.json} contract (contract 2): the single source of truth the TypeScript
 * {@code generate} step consumes. Abstract bases are flattened away (see {@link PlanBuilder}); every
 * runnable entry is concrete. {@code null} fields are omitted on serialization to keep the artifact tidy.
 *
 * @param buildFailed    whether compilation failed; when {@code true}, {@code entries} is empty and
 *                       {@code reason} is set (e.g. {@code "precompile"})
 * @param reason         failure reason when {@code buildFailed}, otherwise {@code null}
 * @param entries        the concrete, runnable (or explicitly skipped) targets
 * @param expansions     one record per abstract base that was expanded, for the report
 * @param taskSelections one record per target whose candidate tasks were capped, for the report
 * @param unresolved     refs that could not be resolved, surfaced rather than silently dropped
 * @param commands       ready batch commands (one per Buildkite batch step), target-neutral (see
 *                       {@link PlanCommand}); the TS generate step maps these straight to steps
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FlakinessPlan(
    boolean buildFailed,
    String reason,
    List<PlanEntry> entries,
    List<Expansion> expansions,
    List<TaskSelection> taskSelections,
    List<Unresolved> unresolved,
    List<PlanCommand> commands
) {

    /** Return a copy with the given batch commands attached (the scan task fills these after enrichment). */
    public FlakinessPlan withCommands(List<PlanCommand> commands) {
        return new FlakinessPlan(buildFailed, reason, entries, expansions, taskSelections, unresolved, commands);
    }

    /**
     * One concrete target. {@code disposition} is {@code "run"} or {@code "skip"}; a {@code skip} carries a
     * {@code reason} (e.g. {@code "requires-packaging-host"}, which the analyze step folds into
     * {@code not_applicable} with that explanation). An entry produced by flattening an abstract base carries
     * {@code expandedFrom} = the abstract FQCN.
     *
     * <p>{@code runnableTasks} are the authoritative task paths that re-run this entry, from the project's
     * real {@code Test} tasks - <em>not</em> the assumed {@code :project:<kind>}. {@link CommandBuilder}
     * builds the gradle invocations straight from them, so a bwc target runs its {@code v<version>#bwcTest}
     * tasks. Empty on a {@code skip} entry.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PlanEntry(
        String gradleProject,
        String sourceSet,
        String kind,
        String fqcn,
        String suitePath,
        String yamlTest,
        String disposition,
        String reason,
        String expandedFrom,
        List<String> runnableTasks
    ) {}

    /** Report record for an abstract base flattened into {@code ran} of {@code total} concrete subclasses. */
    public record Expansion(String abstractFqcn, int ran, int total, int cap) {}

    /**
     * Report record for a target whose candidate tasks were capped: {@code selected} of {@code total}
     * candidates were kept (newest-first). Emitted only when something was dropped, so that a bwc target
     * fanning out to 67 {@code v<version>#bwcTest} tasks visibly reports which 2 ran.
     */
    public record TaskSelection(String gradleProject, String sourceSet, List<String> selected, int total, int cap) {}

    /** A ref that could not be resolved, with a machine-readable {@code reason} (e.g. {@code "no-source-file"}). */
    public record Unresolved(FlakinessRef ref, String reason) {}

    /** The plan emitted when compilation failed: no runnable entries, {@code buildFailed:true}. */
    public static FlakinessPlan buildFailed(String reason) {
        return new FlakinessPlan(true, reason, List.of(), List.of(), List.of(), List.of(), List.of());
    }
}
