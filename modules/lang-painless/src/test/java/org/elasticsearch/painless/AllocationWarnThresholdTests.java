/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.test.MockLog;

/**
 * Behaviour of the allocation warning threshold: it reports a script's allocation without failing it, works with the
 * enforcement limit off entirely, and reports at most once per execution so a hot script cannot flood the log.
 */
public class AllocationWarnThresholdTests extends AllocationTestCase {

    /** A script that allocates well past any small threshold, but is bounded so it always completes. */
    private static final String ALLOCATING = "String s = ''; for (int i = 0; i < 200; ++i) { s = 'abcdefghij'.toUpperCase(); } return s;";

    /** Counts warning-threshold log events, since the stock expectations only record whether one was seen at all. */
    private static class WarnCountingExpectation implements MockLog.LoggingExpectation {

        private int count;

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() == Level.WARN && event.getMessage().getFormattedMessage().contains("allocation warning threshold")) {
                count++;
            }
        }

        @Override
        public void assertMatched() {
            // Counting only; the tests below assert on the count directly.
        }
    }

    /** Runs {@code action} and returns how many warning-threshold WARN events it logged. */
    private static int countWarnings(Runnable action) {
        try (MockLog mockLog = MockLog.capture(AllocationGuard.class)) {
            WarnCountingExpectation counter = new WarnCountingExpectation();
            mockLog.addExpectation(counter);
            action.run();
            // MockLog requires every expectation to be asserted before release, even a purely counting one.
            mockLog.assertAllExpectationsMatched();
            return counter.count;
        }
    }

    public void testWarnsWithLimitOff() {
        // The point of the feature: observe an allocation-heavy script without enforcing anything on it. The script must run
        // to completion and produce its normal result, and still be reported.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        assertEquals(1, countWarnings(() -> assertEquals("ABCDEFGHIJ", script.execute())));
    }

    public void testWarningNamesTheScriptAndItsContext() {
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        MockLog.assertThatLogger(
            script::execute,
            AllocationGuard.class,
            new MockLog.SeenEventExpectation(
                "warning names the script and context",
                AllocationGuard.class.getCanonicalName(),
                Level.WARN,
                "Painless script [" + SCRIPT_NAME + "] in context [" + PainlessTestScript.CONTEXT.name + "]*"
            )
        );
    }

    public void testWarningIncludesTheScriptSource() {
        // The source is what makes the warning actionable without first hunting down the script by name.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        MockLog.assertThatLogger(
            script::execute,
            AllocationGuard.class,
            new MockLog.SeenEventExpectation(
                "warning includes the source",
                AllocationGuard.class.getCanonicalName(),
                Level.WARN,
                "*Source: [" + ALLOCATING + "]*"
            )
        );
    }

    public void testLongSourceIsTruncated() {
        String source = "x".repeat(AllocationGuard.MAX_LOGGED_SOURCE_LENGTH + 50);
        String abbreviated = AllocationGuard.abbreviateSource(source);
        assertTrue(abbreviated.startsWith("x".repeat(AllocationGuard.MAX_LOGGED_SOURCE_LENGTH)));
        assertTrue(abbreviated.contains("truncated from " + source.length() + " chars"));
    }

    public void testShortSourceIsNotTruncated() {
        assertEquals(ALLOCATING, AllocationGuard.abbreviateSource(ALLOCATING));
    }

    public void testDoesNotWarnBelowThreshold() {
        // A threshold far above what the script allocates must stay silent.
        PainlessTestScript script = compile(ALLOCATING, null, "1mb");
        assertEquals(0, countWarnings(script::execute));
    }

    public void testWarnsOncePerExecution() {
        // Once the running total is past the threshold it stays past it, so every one of the 200 charged allocations in this
        // loop would otherwise warn. The $allocWarned latch is what keeps a per-document script from flooding the log.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        assertEquals(1, countWarnings(script::execute));
    }

    public void testWarnsAgainOnNextExecution() {
        // The latch is reset at the execute entry alongside the counter, so a reused instance reports on each execution
        // rather than only the first.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        assertEquals(3, countWarnings(() -> {
            script.execute();
            script.execute();
            script.execute();
        }));
    }

    public void testWarnsAndStillEnforcesWhenBothOn() {
        // With both on, the warning is reported and the limit still fails the script.
        PainlessTestScript script = compile(ALLOCATING, "2kb", "1b");
        assertEquals(1, countWarnings(() -> expectThrows(Exception.class, script::execute)));
    }

    public void testNoWarningWhenOnlyLimitIsOn() {
        // Enforcement alone must not produce warning-threshold events; the limit breach has its own message.
        PainlessTestScript script = compile(ALLOCATING, "2kb");
        assertEquals(0, countWarnings(() -> expectThrows(Exception.class, script::execute)));
    }

    public void testCounterStillTracksInWarningOnlyMode() {
        // Warning alone enables tracking, so the running total must be charged exactly as it is under enforcement.
        PainlessTestScript script = compile(ALLOCATING, null, "1mb");
        script.execute();
        assertTrue(((PainlessScript) script).getAllocBytes() > 0L);
    }
}
