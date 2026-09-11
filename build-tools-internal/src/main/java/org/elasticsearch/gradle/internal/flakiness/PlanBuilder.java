/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.Expansion;
import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.PlanEntry;
import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.TaskSelection;
import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.Unresolved;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Assembles the final {@link FlakinessPlan} from resolved {@link BaseTarget}s and bytecode enrichment. Pure:
 * no Gradle, no I/O beyond what the {@link ClassHierarchyScanner} already did.
 *
 * <p>Rules (contract 2):
 * <ul>
 *   <li>target with a {@code skipReason} (no enabled task, or only packaging-host tasks) -&gt; single
 *       {@code skip} entry carrying that reason (downstream {@code not_applicable}).</li>
 *   <li>yaml kinds (no fqcn, or a specific parameterised case) -&gt; pass through as {@code run}; bytecode
 *       enrichment is a no-op for them.</li>
 *   <li>Java kind with an fqcn -&gt; {@link ClassHierarchyScanner#expand} it. Concrete (or never scanned):
 *       a single {@code run} for the class itself. Abstract: a {@code run} for <b>each</b> concrete subclass
 *       the cap selected (sorted by FQCN, at most {@code subclassCap}, default
 *       {@value #DEFAULT_SUBCLASS_CAP}), every one stamped with {@code expandedFrom}, plus exactly one
 *       {@link Expansion} report record for the base. A chosen subclass that is not a runnable test class -
 *       an inner or anonymous subclass, a helper - becomes a {@code skip} with {@code not-a-test-class}
 *       instead. Abstract with zero concrete subclasses anywhere in the scan set: surfaced as
 *       {@code unresolved}, never emitted as itself.</li>
 *   <li>An expanded subclass compiled <em>outside</em> the abstract base's own source-set output -&gt; re-homed
 *       onto the source set that really owns it. See below.</li>
 * </ul>
 *
 * <p>Every {@code run} entry carries {@code runnableTasks} through to the plan, so {@link CommandBuilder} can
 * build the invocation from real task paths.
 *
 * <h2>Subclasses outside the target's own output</h2>
 * The scan covers the whole repo, so expanding an abstract base routinely turns up concrete subclasses compiled
 * into a different source-set output - another project's, or another source set of the same project. Finding
 * them is the point of compiling everything.
 *
 * <p>They cannot simply inherit the base target's {@code runnableTasks}: those were chosen by
 * {@link TestTaskSelector} by intersecting each {@code Test} task's {@code testClassesDirs} with the base's
 * <em>own</em> output directory, so emitting {@code :app:test --tests com.downstream.DownstreamTests} would
 * match nothing, run zero tests, and be indistinguishable downstream from a hang.
 *
 * <p>Instead each such subclass is <b>re-homed</b>: the scanner reports which directory its bytecode came from,
 * {@link FlakinessTargets#dispositionsByClassDir} maps that directory to the owning project and source set, and
 * the entry is emitted with that project's path, source set, kind and real task paths. It runs under
 * {@code :downstream:test}, which is what actually executes it. This works because <em>every</em> project
 * reports a {@link SourceSetDisposition} per test source set, whether or not it owned a ref.
 *
 * <p>If the owning source set has nothing runnable (bwc-only, packaging host), its own {@code skipReason} is
 * carried through rather than a new one invented. A directory no project claimed falls back to
 * {@value #REASON_SUBCLASS_OUTSIDE_TARGET_OUTPUT}.
 *
 * <p>Since <em>every</em> candidate source set now reports a {@link SourceSetDisposition} (see
 * {@code FlakinessResolveProjectTask#dispositionsOf}), {@code main} is the only scanned directory left that
 * has none - it is in the scan set because abstract bases live there, but refs never resolve into it. So the
 * fallback survives only for a concrete subclass compiled into a {@code main} output, which cannot happen: a
 * {@code main} source set cannot depend on a test source set. Audited over the whole repo: zero of 532
 * abstract bases have a descendant in a {@code main} output.
 *
 * <p>That argument is about {@code main} and nothing else, so it is only sound while the disposition set and
 * the scan set agree. It did not hold before: {@code yamlRestTest} outputs were scanned but disposition-less,
 * and a ref on {@code AbstractXPackRestTest} - whose subclasses are all yaml runners - produced five of these
 * skips and no runnable work at all.
 *
 * <p>The comparison is on the compiled-output directory rather than the Gradle project path on purpose: a base
 * in {@code :p}'s {@code test} source set and a subclass in {@code :p}'s {@code internalClusterTest} source set
 * share a project but not a {@code Test} task, so comparing projects would wrongly reuse the base's tasks.
 */
public final class PlanBuilder {

    public static final int DEFAULT_SUBCLASS_CAP = 5;

    /**
     * A compiled-output directory that no project claimed a source set for, so the subclass found in it cannot
     * be attributed to any {@code Test} task. Kept as a genuine fallback rather than an assertion: {@code main}
     * outputs are scanned but carry no disposition, and it is the only such directory (see the class javadoc
     * for why that makes it unreachable in practice, and for the yamlRestTest case where it was not).
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
                // A subclass from anywhere else is re-homed onto its own source set's tasks.
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
     * real {@code Test} tasks, rather than the base target's (which do not run it).
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
