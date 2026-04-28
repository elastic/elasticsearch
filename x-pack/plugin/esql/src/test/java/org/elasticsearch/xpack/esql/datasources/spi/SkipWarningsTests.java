/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class SkipWarningsTests extends ESTestCase {

    public void testEmitsSummaryOnFirstAddOnly() {
        SkipWarnings warnings = new SkipWarnings("the summary");
        warnings.add("first detail");
        warnings.add("second detail");

        List<String> emitted = drain();
        assertEquals(3, emitted.size());
        assertEquals("the summary", emitted.get(0));
        assertEquals("first detail", emitted.get(1));
        assertEquals("second detail", emitted.get(2));
    }

    public void testNoSummaryWhenNothingAdded() {
        new SkipWarnings("not used");
        assertNull(threadContext.getResponseHeaders().get("Warning"));
    }

    public void testCapsAtMaxAddedWarningsAndEmitsOverflow() {
        SkipWarnings warnings = new SkipWarnings("summary");
        int total = SkipWarnings.MAX_ADDED_WARNINGS + 5;
        for (int i = 0; i < total; i++) {
            warnings.add("detail " + i);
        }

        List<String> emitted = drain();
        // 1 summary + MAX_ADDED_WARNINGS details + 1 overflow notice
        assertEquals(SkipWarnings.MAX_ADDED_WARNINGS + 2, emitted.size());
        assertEquals("summary", emitted.get(0));
        assertEquals("detail 0", emitted.get(1));
        assertEquals("detail " + (SkipWarnings.MAX_ADDED_WARNINGS - 1), emitted.get(SkipWarnings.MAX_ADDED_WARNINGS));
        String overflow = emitted.get(SkipWarnings.MAX_ADDED_WARNINGS + 1);
        assertTrue("overflow should mention suppression, got: " + overflow, overflow.contains("further warnings suppressed"));
    }

    public void testOverflowEmittedOnceEvenWithManyExtraAdds() {
        SkipWarnings warnings = new SkipWarnings("s");
        for (int i = 0; i < SkipWarnings.MAX_ADDED_WARNINGS + 50; i++) {
            warnings.add("d");
        }
        // The "d" details dedupe inside HeaderWarning's identical-message cache, so we cannot count
        // them by emission. What we CAN check is that the overflow message appears exactly once.
        List<String> emitted = drain();
        long overflowCount = emitted.stream().filter(s -> s.contains("further warnings suppressed")).count();
        assertEquals(1, overflowCount);
    }

    public void testNoopDropsEverything() {
        SkipWarnings.NOOP.add("first");
        SkipWarnings.NOOP.add("second");
        assertNull(threadContext.getResponseHeaders().get("Warning"));
    }

    public void testOfStrictPolicyReturnsNoop() {
        assertSame(SkipWarnings.NOOP, SkipWarnings.of(ErrorPolicy.STRICT, "summary ignored"));
    }

    public void testOfLenientPolicyReturnsLiveCollector() {
        SkipWarnings warnings = SkipWarnings.of(ErrorPolicy.LENIENT, "live summary");
        assertNotSame(SkipWarnings.NOOP, warnings);
        warnings.add("a detail");
        List<String> emitted = drain();
        assertEquals(2, emitted.size());
        assertEquals("live summary", emitted.get(0));
        assertEquals("a detail", emitted.get(1));
    }

    /**
     * Pin {@link SkipWarnings#MAX_ADDED_WARNINGS} to the same value used by
     * {@code compute.operator.Warnings#MAX_ADDED_WARNINGS} (package-private, not importable here).
     * Keep the literal below in sync if the compute-side cap ever changes.
     */
    public void testMaxAddedWarningsMatchesComputeOperatorWarningsCap() {
        assertEquals(
            "SkipWarnings.MAX_ADDED_WARNINGS must stay in sync with compute.operator.Warnings.MAX_ADDED_WARNINGS",
            20,
            SkipWarnings.MAX_ADDED_WARNINGS
        );
    }

    private List<String> drain() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
    }
}
