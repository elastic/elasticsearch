/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * End-to-end allocation-limit tests using scripts shaped like ones a user might actually write. Each is a runaway that
 * <em>retains</em> what it allocates (a growing string, a growing list) — the true heap-exhaustion shape — and is preempted by
 * the limit before the heap fills.
 * <p>
 * Because these hold the allocated memory live until the trip, the limit is kept well under the standard 512m test heap
 * ({@code 64mb} here, ~12% of the heap) so the live set at the trip point is comfortably bounded and the test can never flake on
 * an OOM. (Cumulative, bounded charging is covered exactly in {@link AllocationDefDispatchTests}.)
 */
public class AllocationRealisticScriptTests extends AllocationTestCase {

    public void testRunawayStringConcatDoublingIsPreempted() {
        // The classic accidental OOM: repeatedly doubling a string (each result is retained as the new s). The static-type
        // concat pre-check charges each result and trips within a couple dozen iterations, long before the loop's 1000 rounds.
        assertTripsLimit("""
            String s = "wat";
            for (int i = 0; i < 1000; ++i) {
                s = s + s;
            }
            return s;
            """, "64mb");
    }

    public void testRunawayDefListBuildingIsPreempted() {
        // A user accumulating def-dispatched slices into a list that is never released. Each retained substring is charged, so
        // the list is preempted once the charged total crosses the limit — with only ~64MB live at the trip.
        assertTripsLimit("""
            def parts = new ArrayList();
            def s = String.copyValueOf(new char[100000]);
            for (int i = 0; i < 100000000; ++i) {
                parts.add(s.substring(0, 100000));
            }
            return parts.size();
            """, "64mb");
    }
}
