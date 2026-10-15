/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness.scan;

import org.elasticsearch.gradle.internal.flakiness.FlakinessProperties;
import org.elasticsearch.gradle.internal.flakiness.model.BaseTarget;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan.Expansion;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan.PlanEntry;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan.TaskSelection;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan.Unresolved;
import org.elasticsearch.gradle.internal.flakiness.model.FlakinessRef;
import org.elasticsearch.gradle.internal.flakiness.model.Kinds;
import org.elasticsearch.gradle.internal.flakiness.model.SourceSetDisposition;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Assembles the final {@link FlakinessPlan} from resolved {@link BaseTarget}s and bytecode enrichment.
 * Pure: no Gradle access or I/O beyond what the {@link ClassHierarchyScanner} already performed.
 * See the {@link org.elasticsearch.gradle.internal.flakiness.scan package description} for the phase boundary.
 */
public final class PlanBuilder {

    /**
     * A compiled-output directory that no project claimed a source set for, so the subclass found in it cannot
     * be attributed to any {@code Test} task. Kept as a genuine fallback rather than an assertion: {@code main}
     * outputs are scanned because abstract bases live there, but refs never resolve into {@code main}.
     * A concrete subclass in {@code main} cannot depend on a test source set, so this fallback should not
     * arise while every scanned test source set reports a disposition.
     * That reasoning only holds while the scan and disposition sets agree:
     * omitting {@code yamlRestTest} dispositions, for example, makes its subclasses unattributable. A ref on
     * {@code AbstractXPackRestTest}, whose subclasses are yaml runners, then produces five of these skips
     * instead of runnable work.
     */
    public static final String REASON_SUBCLASS_OUTSIDE_TARGET_OUTPUT = "subclass-outside-target-output";

    /**
     * The class is not something a {@code Test} task can run - a helper, fixture or mock that happens to live
     * in a test source set, or an inner/anonymous subclass surfaced by bytecode expansion. See
     * {@link TestClassNames}. Reported rather than dropped so a mis-named real test is visible instead of
     * silently missing from the run.
     */
    public static final String REASON_NOT_A_TEST_CLASS = "not-a-test-class";

    private PlanBuilder() {}

    /**
     * A skipped target produces one skip entry; yaml targets pass through; concrete Java targets produce one
     * run entry. Abstract Java targets expand into at most {@code subclassCap} runnable subclasses
     * (default {@value FlakinessProperties#DEFAULT_SUBCLASS_CAP}, ordered by FQCN), each stamped with
     * {@code expandedFrom}, plus one {@link Expansion} report. Non-test descendants become reported skips,
     * and an abstract base with no concrete descendants is unresolved rather than emitted as a runnable class.
     * Every run entry retains the selected task paths for {@link CommandBuilder}.
     *
     * @param dispositionOfClassDir maps a compiled-output directory to the project + source set that owns it
     *                              (see {@link FlakinessTargets#dispositionsByClassDir}), so an expanded
     *                              subclass found outside the base target's own output can be run by the tasks
     *                              that really execute it
     */
    public static FlakinessPlan build(
        List<BaseTarget> targets,
        List<Unresolved> unresolvedIn,
        ClassHierarchyScanner scanner,
        int subclassCap,
        int taskCap,
        Function<Path, FlakinessTargets.OwnedSourceSet> dispositionOfClassDir
    ) {
        List<PlanEntry> entries = new ArrayList<>();
        List<Expansion> expansions = new ArrayList<>();
        List<TaskSelection> taskSelections = new ArrayList<>();
        List<Unresolved> unresolved = new ArrayList<>(unresolvedIn);
        // One report record per (project, sourceSet): every target of the same source set saw the same
        // candidate tasks, so repeating it per target would just be noise.
        Set<String> reportedSelections = new LinkedHashSet<>();

        for (BaseTarget t : targets) {
            if (t.runnable() == false) {
                entries.add(skip(t, t.skipReason()));
                continue;
            }
            // add to report if something was capped, and we haven't yet reported this source set
            if (t.candidateTasks() > t.runnableTasks().size() && reportedSelections.add(t.gradleProject() + "|" + t.sourceSet())) {
                taskSelections.add(new TaskSelection(t.gradleProject(), t.sourceSet(), t.runnableTasks(), t.candidateTasks(), taskCap));
            }

            if (Kinds.BYTECODE_ENRICHED.contains(t.kind()) == false || t.fqcn() == null) {
                // yaml suite/runner/case: nothing to enrich, run as-is.
                entries.add(run(t, t.fqcn(), null));
                continue;
            }
            ClassHierarchyScanner.Expansion ex = scanner.expand(t.fqcn(), subclassCap);
            if (ex.wasAbstract()) {
                if (ex.classesToRun().isEmpty() && ex.notTests().isEmpty()) {
                    // An abstract base with no concrete subclass on the classpath is nothing to run; do not
                    // silently drop it.
                    unresolved.add(
                        new Unresolved(
                            new FlakinessRef(FlakinessRef.SOURCE_UNMUTE, null, t.fqcn(), null, null),
                            FlakinessPlan.REASON_ABSTRACT_NO_CONCRETE_SUBCLASS
                        )
                    );
                    continue;
                }
                expansions.add(new Expansion(t.fqcn(), ex.classesToRun().size(), ex.totalRunnable(), subclassCap));
                // The base's runnableTasks were selected by intersecting each Test task's testClassesDirs with
                // the base's OWN source-set output, so they only run classes compiled into that same directory.
                // A subclass from anywhere else is re-homed onto its own source set's tasks. Compare directories,
                // not projects: :p's test and internalClusterTest source sets share a project but not a Test task.
                Path baseDir = scanner.originDir(t.fqcn());
                for (String concrete : ex.classesToRun()) {
                    Path dir = scanner.originDir(concrete);
                    if (baseDir == null || baseDir.equals(dir)) {
                        entries.add(run(t, concrete, t.fqcn()));
                        continue;
                    }
                    entries.add(foreign(t, concrete, dispositionOfClassDir.apply(dir)));
                }
                // Concrete in bytecode is not the same as runnable by a Test task: expanding an abstract
                // HELPER yields its inner/anonymous subclasses, and `--tests Foo$1` matches nothing. The
                // scanner has already kept these out of the capped set, so they cost no test execution and
                // displace no real subclass; they are reported here so a mis-named real test stays visible.
                for (String notATest : ex.notTests()) {
                    entries.add(skip(t, notATest, REASON_NOT_A_TEST_CLASS, t.fqcn()));
                }
            } else if (TestClassNames.isRunnableTestClass(t.fqcn()) == false) {
                // A concrete non-test file that happens to live in a test source set. Emitting it would
                // produce `--tests SomeHelper`, which matches nothing and reads downstream as a hang.
                entries.add(skip(t, REASON_NOT_A_TEST_CLASS));
            } else {
                entries.add(run(t, ex.classesToRun().getFirst(), null));
            }
        }
        // Batch commands are attached by the caller (FlakinessScanTask) via withCommands, once it has the
        // iteration config; PlanBuilder stays focused on entry assembly.
        return new FlakinessPlan(false, null, entries, expansions, taskSelections, unresolved, List.of());
    }

    /**
     * A concrete subclass whose bytecode was compiled somewhere other than the base target's own source-set
     * output, re-homed onto the source set that really owns it: the owning project's path, source set, kind and
     * real {@code Test} tasks, rather than the base target's (which do not run it). The scan uses the bytecode
     * origin directory and {@link FlakinessTargets#dispositionsByClassDir} to find the owning source set.
     * For example, {@code :app:test --tests com.downstream.DownstreamTests} would match no tests if that class
     * was compiled in {@code :downstream}; re-homing runs it under {@code :downstream:test} instead. Without
     * this, a zero-test invocation could be misreported as a hang.
     * If that source set has no runnable tasks, carry its own skip reason instead of inventing another one.
     *
     * @param owner the disposition reported by whichever project owns that output directory, or {@code null}
     *              if no project claimed it - which should not happen once every project reports its source
     *              sets, so it is surfaced as a skip rather than guessed at
     */
    private static PlanEntry foreign(BaseTarget t, String fqcn, FlakinessTargets.OwnedSourceSet owner) {
        if (owner == null) {
            return skip(t, fqcn, REASON_SUBCLASS_OUTSIDE_TARGET_OUTPUT, t.fqcn());
        }
        SourceSetDisposition d = owner.disposition();
        if (d.runnable() == false) {
            // The owning source set has nothing that can run it here (bwc-only, packaging host, ...). Carry
            // that project's own reason rather than inventing one.
            return new PlanEntry(
                owner.projectPath(),
                d.sourceSet(),
                d.kind(),
                fqcn,
                null,
                null,
                Kinds.DISPOSITION_SKIP,
                d.skipReason(),
                t.fqcn(),
                List.of()
            );
        }
        return new PlanEntry(
            owner.projectPath(),
            d.sourceSet(),
            d.kind(),
            fqcn,
            null,
            null,
            Kinds.DISPOSITION_RUN,
            null,
            t.fqcn(),
            d.runnableTasks()
        );
    }

    private static PlanEntry run(BaseTarget t, String fqcn, String expandedFrom) {
        return new PlanEntry(
            t.gradleProject(),
            t.sourceSet(),
            t.kind(),
            fqcn,
            t.suitePath(),
            t.yamlTest(),
            Kinds.DISPOSITION_RUN,
            null,
            expandedFrom,
            t.runnableTasks()
        );
    }

    /** A skip for the target itself, so there is no abstract base to attribute it to. */
    private static PlanEntry skip(BaseTarget t, String reason) {
        return skip(t, t.fqcn(), reason, null);
    }

    /**
     * A skip for a specific class rather than the target's own fqcn - used for an expanded subclass, so the
     * plan names the subclass that could not be run instead of the abstract base it came from.
     *
     * @param expandedFrom the abstract base this class was expanded from, or {@code null} if the entry is the
     *                     target itself. Without it a reader cannot tell why a class nobody touched appears
     *                     in the plan at all, which is exactly the case that needs explaining: the class is
     *                     named, but the project and source set are the <em>base's</em>, since there was no
     *                     owning source set to attribute it to.
     */
    private static PlanEntry skip(BaseTarget t, String fqcn, String reason, String expandedFrom) {
        return new PlanEntry(
            t.gradleProject(),
            t.sourceSet(),
            t.kind(),
            fqcn,
            t.suitePath(),
            t.yamlTest(),
            Kinds.DISPOSITION_SKIP,
            reason,
            expandedFrom,
            List.of()
        );
    }
}
