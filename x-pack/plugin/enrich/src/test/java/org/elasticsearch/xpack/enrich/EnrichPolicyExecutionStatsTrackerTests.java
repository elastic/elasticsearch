/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import static org.junit.Assert.assertNotNull;

public class EnrichPolicyExecutionStatsTrackerTests extends ESTestCase {

    public void testEmptyStats() {
        EnrichPolicyExecutionStatsTracker tracker = new EnrichPolicyExecutionStatsTracker();
        EnrichStatsAction.Response.ExecutionStats stats = tracker.getStats();
        assertNotNull(stats);
        assertEquals(0L, stats.getTotalExecutionCount());
        assertEquals(0L, stats.getTotalExecutionTime());
        assertEquals(0L, stats.getMinExecutionTime());
        assertEquals(0L, stats.getMaxExecutionTime());
        assertEquals(0L, stats.getTotalRepeatExecutionCount());
        assertEquals(0L, stats.getTotalTimeBetweenExecutions());
        assertEquals(0L, stats.getAvgTimeBetweenExecutions());
        assertEquals(0L, stats.getMinTimeBetweenExecutions());
        assertEquals(0L, stats.getMaxTimeBetweenExecutions());
    }

    public void testSingleRunStat() {
        EnrichPolicyExecutionStatsTracker tracker = new EnrichPolicyExecutionStatsTracker();
        tracker.captureCompletedExecution("test", 1000, 1025);
        EnrichStatsAction.Response.ExecutionStats stats = tracker.getStats();
        assertNotNull(stats);
        assertEquals(1L, stats.getTotalExecutionCount());
        assertEquals(25L, stats.getTotalExecutionTime());
        assertEquals(25L, stats.getMinExecutionTime());
        assertEquals(25L, stats.getMaxExecutionTime());
        assertEquals(0L, stats.getTotalRepeatExecutionCount());
        assertEquals(0L, stats.getTotalTimeBetweenExecutions());
        assertEquals(0L, stats.getAvgTimeBetweenExecutions());
        assertEquals(0L, stats.getMinTimeBetweenExecutions());
        assertEquals(0L, stats.getMaxTimeBetweenExecutions());
    }

    public void testUniqueRunStats() {
        EnrichPolicyExecutionStatsTracker tracker = new EnrichPolicyExecutionStatsTracker();
        tracker.captureCompletedExecution("test1", 1000, 1025);
        tracker.captureCompletedExecution("test2", 1050, 1100);
        EnrichStatsAction.Response.ExecutionStats stats = tracker.getStats();
        assertNotNull(stats);
        assertEquals(2L, stats.getTotalExecutionCount());
        assertEquals(75L, stats.getTotalExecutionTime());
        assertEquals(25L, stats.getMinExecutionTime());
        assertEquals(50L, stats.getMaxExecutionTime());
        assertEquals(0L, stats.getTotalRepeatExecutionCount());
        assertEquals(0L, stats.getTotalTimeBetweenExecutions());
        assertEquals(0L, stats.getAvgTimeBetweenExecutions());
        assertEquals(0L, stats.getMinTimeBetweenExecutions());
        assertEquals(0L, stats.getMaxTimeBetweenExecutions());
    }

    public void testMultipleRunStats() {
        EnrichPolicyExecutionStatsTracker tracker = new EnrichPolicyExecutionStatsTracker();
        tracker.captureCompletedExecution("test", 1000, 1050);
        tracker.captureCompletedExecution("test", 1100, 1150);
        EnrichStatsAction.Response.ExecutionStats stats = tracker.getStats();
        assertNotNull(stats);
        assertEquals(2L, stats.getTotalExecutionCount());
        assertEquals(100L, stats.getTotalExecutionTime());
        assertEquals(50L, stats.getMinExecutionTime());
        assertEquals(50L, stats.getMaxExecutionTime());
        assertEquals(1L, stats.getTotalRepeatExecutionCount());
        assertEquals(50L, stats.getTotalTimeBetweenExecutions());
        assertEquals(50L, stats.getAvgTimeBetweenExecutions());
        assertEquals(50L, stats.getMinTimeBetweenExecutions());
        assertEquals(50L, stats.getMaxTimeBetweenExecutions());
    }

    public void testRepeatedRunStats() {
        EnrichPolicyExecutionStatsTracker tracker = new EnrichPolicyExecutionStatsTracker();
        tracker.captureCompletedExecution("test", 1000, 1050);
        tracker.captureCompletedExecution("test", 1075, 1125);
        tracker.captureCompletedExecution("test", 1200, 1250);
        tracker.captureCompletedExecution("test", 1300, 1350);
        tracker.captureCompletedExecution("test", 1400, 1450);
        EnrichStatsAction.Response.ExecutionStats stats = tracker.getStats();
        assertNotNull(stats);
        assertEquals(5L, stats.getTotalExecutionCount());
        assertEquals(250L, stats.getTotalExecutionTime());
        assertEquals(50L, stats.getMinExecutionTime());
        assertEquals(50L, stats.getMaxExecutionTime());
        assertEquals(4L, stats.getTotalRepeatExecutionCount());
        assertEquals(200L, stats.getTotalTimeBetweenExecutions());
        assertEquals(50L, stats.getAvgTimeBetweenExecutions());
        assertEquals(25L, stats.getMinTimeBetweenExecutions());
        assertEquals(75L, stats.getMaxTimeBetweenExecutions());
    }

}
