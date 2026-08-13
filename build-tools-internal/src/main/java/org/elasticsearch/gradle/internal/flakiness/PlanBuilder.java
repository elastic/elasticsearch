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
import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.Unresolved;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles the final {@link FlakinessPlan} from resolved {@link BaseTarget}s, bytecode enrichment, and
 * bwc facts. Pure: no Gradle, no I/O beyond what the {@link ClassHierarchyScanner} already did.
 *
 * <p>Rules (contract 2):
 * <ul>
 *   <li>bwc target -&gt; single {@code skip} entry, {@code reason="bwc"} (downstream {@code not_applicable}).</li>
 *   <li>yaml kinds (no fqcn, or a specific parameterised case) -&gt; pass through as {@code run}; bytecode
 *       enrichment is a no-op for them.</li>
 *   <li>Java kind with an fqcn -&gt; {@link ClassHierarchyScanner#expand} it. Concrete: one {@code run}.
 *       Abstract: one {@code run} per chosen concrete subclass with {@code expandedFrom}, plus an
 *       {@link Expansion} record. Abstract with zero concrete subclasses: surfaced as {@code unresolved}.</li>
 * </ul>
 */
public final class PlanBuilder {

    public static final int DEFAULT_SUBCLASS_CAP = 5;

    private PlanBuilder() {}

    public static FlakinessPlan build(
        List<BaseTarget> targets,
        List<Unresolved> unresolvedIn,
        ClassHierarchyScanner scanner,
        int subclassCap
    ) {
        List<PlanEntry> entries = new ArrayList<>();
        List<Expansion> expansions = new ArrayList<>();
        List<Unresolved> unresolved = new ArrayList<>(unresolvedIn);

        for (BaseTarget t : targets) {
            if (t.bwc()) {
                entries.add(skip(t, "bwc"));
                continue;
            }
            if (Kinds.BYTECODE_ENRICHED.contains(t.kind()) == false || t.fqcn() == null) {
                // yaml suite/runner/case: nothing to enrich, run as-is.
                entries.add(run(t, t.fqcn(), null));
                continue;
            }
            ClassHierarchyScanner.Expansion ex = scanner.expand(t.fqcn(), subclassCap);
            if (ex.wasAbstract()) {
                if (ex.toRun().isEmpty()) {
                    // An abstract base with no concrete subclass on the classpath is nothing to run; do not
                    // silently drop it.
                    unresolved.add(
                        new Unresolved(
                            new FlakinessRef(FlakinessRef.SOURCE_UNMUTE, null, t.fqcn(), null, null),
                            "abstract-no-concrete-subclass"
                        )
                    );
                    continue;
                }
                expansions.add(new Expansion(t.fqcn(), ex.toRun().size(), ex.totalConcrete(), subclassCap));
                for (String concrete : ex.toRun()) {
                    entries.add(run(t, concrete, t.fqcn()));
                }
            } else {
                entries.add(run(t, ex.toRun().get(0), null));
            }
        }
        // Batch commands are attached by the caller (FlakinessScanTask) via withCommands, once it has the
        // iteration config; PlanBuilder stays focused on entry assembly.
        return new FlakinessPlan(false, null, entries, expansions, unresolved, List.of());
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
            expandedFrom
        );
    }

    private static PlanEntry skip(BaseTarget t, String reason) {
        return new PlanEntry(
            t.gradleProject(),
            t.sourceSet(),
            t.kind(),
            t.fqcn(),
            t.suitePath(),
            t.yamlTest(),
            Kinds.DISPOSITION_SKIP,
            reason,
            null
        );
    }
}
