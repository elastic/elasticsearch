/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.profile;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for Lance timing infrastructure.
 */
public class LanceTimingTests {

    @Test
    public void testTimingCollectionWhenDisabled() {
        // Timing should not be collected when not activated
        LanceTimingContext context = LanceTimingContext.getOrCreate();

        // Don't activate - timing should be inactive
        assertThat(context.isActive(), Matchers.is(false));

        // Record some timing (should be ignored)
        context.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 100);

        // Output should be all zeros
        Map<String, Object> timing = context.toDebugMap();
        assertThat((Long) timing.get("lance_native_search_execution_ms"), Matchers.is(0L));

        // Clean up
        LanceTimingContext.remove();
    }

    @Test
    public void testTimingCollectionWhenEnabled() {
        // Timing should be collected when activated
        LanceTimingContext context = LanceTimingContext.getOrCreate();
        context.activate();

        assertThat(context.isActive(), Matchers.is(true));

        // Record some timing
        context.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 100);
        context.record(LanceTimingContext.LanceTimingStage.ID_MATCHING, 50);
        context.record(LanceTimingContext.LanceTimingStage.ID_MATCHING, 25);  // Multiple recordings

        // Verify timing was recorded
        Map<String, Object> timing = context.toDebugMap();
        assertThat((Long) timing.get("lance_native_search_execution_ms"), Matchers.is(100L));
        assertThat((Long) timing.get("lance_id_matching_ms"), Matchers.is(75L)); // Sum of both recordings

        // Clean up
        context.clear();
        LanceTimingContext.remove();
    }

    @Test
    public void testTimerRecordsCorrectly() {
        LanceTimingContext context = LanceTimingContext.getOrCreate();
        context.activate();

        // Use timer to record some work
        try (var timer = new LanceTimer(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION)) {
            // Simulate some work
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Check that timing was recorded (at least 10ms)
        Map<String, Object> timing = context.toDebugMap();
        Long recordedTime = (Long) timing.get("lance_native_search_execution_ms");
        assertThat(recordedTime, Matchers.greaterThanOrEqualTo(10L));

        // Clean up
        context.clear();
        LanceTimingContext.remove();
    }

    @Test
    public void testTimingContextClearedBetweenQueries() {
        LanceTimingContext context = LanceTimingContext.getOrCreate();
        context.activate();

        // Record some timing
        context.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 100);

        // Clear the context
        context.clear();

        // Check that timing is reset
        Map<String, Object> timing = context.toDebugMap();
        assertThat((Long) timing.get("lance_native_search_execution_ms"), Matchers.is(0L));

        // Clean up
        LanceTimingContext.remove();
    }

    @Test
    public void testThreadLocalIsolation() throws Exception {
        // Create two threads and verify they don't share timing data
        LanceTimingContext context1 = LanceTimingContext.getOrCreate();
        context1.activate();

        Thread thread = new Thread(() -> {
            LanceTimingContext context2 = LanceTimingContext.getOrCreate();
            context2.activate();
            context2.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 200);
            context2.clear();
            LanceTimingContext.remove();
        });

        thread.start();
        thread.join();

        // Record in thread 1
        context1.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 100);

        // Verify thread 1 only has its own timing
        Map<String, Object> timing = context1.toDebugMap();
        assertThat((Long) timing.get("lance_native_search_execution_ms"), Matchers.is(100L));

        // Clean up
        context1.clear();
        LanceTimingContext.remove();
    }

    @Test
    public void testTimingCategorization() {
        LanceTimingContext context = LanceTimingContext.getOrCreate();
        context.activate();

        // Record one-time costs
        context.record(LanceTimingContext.LanceTimingStage.URI_SETUP, 10);
        context.record(LanceTimingContext.LanceTimingStage.NATIVE_DATASET_OPEN, 50);

        // Record per-search costs
        context.record(LanceTimingContext.LanceTimingStage.NATIVE_SEARCH_EXECUTION, 100);
        context.record(LanceTimingContext.LanceTimingStage.ID_MATCHING, 25);

        Map<String, Object> timing = context.toDebugMap();

        // Verify one-time total
        assertThat((Long) timing.get("lance_total_onetime_ms"), Matchers.is(60L));

        // Verify per-search total
        assertThat((Long) timing.get("lance_total_persearch_ms"), Matchers.is(125L));

        // Verify overall total
        assertThat((Long) timing.get("lance_total_ms"), Matchers.is(185L));

        // Clean up
        context.clear();
        LanceTimingContext.remove();
    }

    @Test
    public void testStageFieldNames() {
        // Verify that all stages have correct field names
        for (LanceTimingContext.LanceTimingStage stage : LanceTimingContext.LanceTimingStage.values()) {
            String fieldName = stage.getFieldName();
            assertThat(fieldName, Matchers.startsWith("lance_"));
            assertThat(fieldName, Matchers.endsWith("_ms"));
        }
    }

    @Test
    public void testOneTimeVsPerSearchStages() {
        // Verify stage categorization
        int oneTimeCount = 0;
        int perSearchCount = 0;

        for (LanceTimingContext.LanceTimingStage stage : LanceTimingContext.LanceTimingStage.values()) {
            if (stage.isOneTime()) {
                oneTimeCount++;
            } else {
                perSearchCount++;
            }
        }

        // We have 4 one-time stages and 9 per-search stages
        assertThat(oneTimeCount, Matchers.is(4));
        assertThat(perSearchCount, Matchers.is(9));
    }
}
