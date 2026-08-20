/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.script.ScriptException;

/**
 * Realistic end-to-end scenarios: scripts a user might actually write whose allocation runs away in a loop. Under a sane
 * per-context limit the pre-checks trip mid-loop and fail the script rather than letting it OOM the node, while ordinary
 * bounded scripts run to completion. These exercise the annotations from this change (and the operator-concat pre-check from
 * the earlier series PR) the way production scripts would hit them.
 */
public class AllocationScenarioTests extends AllocationTestCase {

    /** Asserts running {@code source} under a realistic {@code limit} trips the allocation limit rather than completing. */
    private void assertTripsUnder(String source, String limit) {
        PainlessTestScript script = compile(source, limit);
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error for [" + source + "], but got: " + e, e);
    }

    public void testRunawayStringConcatOperatorTripsLimit() {
        // The classic accidental exponential blow-up: s = s + s doubles the string each iteration. The static-type concat
        // pre-check charges each result and trips within ~20 iterations, long before the 1000-iteration loop could OOM.
        assertTripsUnder("String s = 'wat'; for (int i = 0; i < 1000; ++i) { s = s + s; } return s;", "1mb");
    }

    public void testRunawayConcatMethodTripsLimit() {
        // Same blow-up expressed with String.concat(String) (annotated in this change) instead of the + operator.
        assertTripsUnder("String s = 'wat'; for (int i = 0; i < 1000; ++i) { s = s.concat(s); } return s;", "1mb");
    }

    public void testRepeatedTransientStringAllocationTripsLimit() {
        // Not exponential, but a hot loop that allocates a fresh String every iteration. The counter is cumulative over the
        // execution (it charges what is allocated, not what survives), so the transient garbage still trips the limit.
        assertTripsUnder(
            "String base = 'the quick brown fox jumps'; String s = ''; "
                + "for (int i = 0; i < 1000000; ++i) { s = base.toUpperCase(); } return s;",
            "1mb"
        );
    }

    public void testRunawayCollectionCopyTripsLimit() {
        // Accidental quadratic: copying a list that doubles each iteration. The copy constructor charge scales with the
        // growing source, so it trips before the copies exhaust the heap.
        assertTripsUnder(
            "List l = new ArrayList(); l.add(1); for (int i = 0; i < 1000; ++i) { l.addAll(new ArrayList(l)); } return l.size();",
            "1mb"
        );
    }

    public void testTypicalStringManipulationCompletes() {
        // A normal script using several annotated String methods stays far under the limit and runs to completion unaffected.
        PainlessTestScript script = compile(
            "String s = 'Hello World'; return s.toLowerCase().concat(s.toUpperCase().substring(0, 5));",
            "1mb"
        );
        assertEquals("hello worldHELLO", script.execute());
    }
}
