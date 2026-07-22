/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins which execs carry {@link RowCountPreserving}. Partition-pruning seed propagation trusts this marker to decide
 * whether a filter may be pushed past a node ({@code PartitionPruningRule#rowPreserving(PhysicalPlan)}), so marking a
 * node that actually changes the row count would prune away matching rows. The forgotten-marker direction is caught by
 * the pruning tests (a full scan is correct, only slow); this test guards the dangerous direction — a
 * cardinality-changing exec must never carry the marker.
 */
public class RowCountPreservingTests extends ESTestCase {

    public void testRowCountPreservingExecsAreMarked() {
        assertMarked(FilterExec.class);   // only removes rows; a filter below is still sound
        assertMarked(EvalExec.class);     // adds columns, one row in one row out
        assertMarked(ProjectExec.class);  // column selection — the lowered form of KEEP/DROP/RENAME
        assertMarked(RegexExtractExec.class);
        assertMarked(DissectExec.class);  // extends RegexExtractExec
        assertMarked(GrokExec.class);     // extends RegexExtractExec
        assertMarked(EnrichExec.class);
    }

    public void testCardinalityChangingExecsAreNotMarked() {
        assertNotMarked(LimitExec.class);
        assertNotMarked(TopNExec.class);
        assertNotMarked(AggregateExec.class);
        assertNotMarked(SampleExec.class);
        assertNotMarked(MvExpandExec.class);
    }

    private static void assertMarked(Class<? extends PhysicalPlan> exec) {
        assertTrue(exec.getSimpleName() + " must carry RowCountPreserving", RowCountPreserving.class.isAssignableFrom(exec));
    }

    private static void assertNotMarked(Class<? extends PhysicalPlan> exec) {
        assertFalse(
            exec.getSimpleName() + " changes the row count and must NOT carry RowCountPreserving",
            RowCountPreserving.class.isAssignableFrom(exec)
        );
    }
}
