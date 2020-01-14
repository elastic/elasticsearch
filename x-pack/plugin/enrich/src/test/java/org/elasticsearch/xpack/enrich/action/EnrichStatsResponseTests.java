/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutionStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnrichStatsResponseTests extends AbstractWireSerializingTestCase<EnrichStatsAction.Response> {

    @Override
    protected EnrichStatsAction.Response createTestInstance() {
        int numExecutingPolicies = randomIntBetween(0, 16);
        List<ExecutingPolicy> executingPolicies = new ArrayList<>(numExecutingPolicies);
        for (int i = 0; i < numExecutingPolicies; i++) {
            TaskInfo taskInfo = randomTaskInfo();
            executingPolicies.add(new ExecutingPolicy(randomAlphaOfLength(4), taskInfo));
        }
        ExecutionStats executionStats = randomExecutionStats();
        List<CoordinatorStats> coordinatorStats = randomCoordinatorStats();
        return new EnrichStatsAction.Response(executingPolicies, executionStats, coordinatorStats);
    }

    public static ExecutionStats randomExecutionStats() {
        long totalExecutionCount;
        long totalExecutionTime;
        long minExecutionTime;
        long maxExecutionTime;
        long totalRepeatExecutionCount;
        long totalTimeBetweenExecutions;
        long avgTimeBetweenExecutions;
        long minTimeBetweenExecutions;
        long maxTimeBetweenExecutions;

        boolean executions = randomBoolean();
        if (executions) {
            // random executions
            totalExecutionCount = randomIntBetween(1, 1000);
            totalExecutionTime = randomLongBetween(totalExecutionCount, Long.MAX_VALUE);
            minExecutionTime = randomIntBetween(1, 1000000);
            maxExecutionTime = randomLongBetween(minExecutionTime, Long.MAX_VALUE);
        } else {
            // no executions at all
            totalExecutionCount = 0L;
            totalExecutionTime = 0L;
            minExecutionTime = -1L;
            maxExecutionTime = 0L;
        }

        boolean hadRepeatRuns = randomBoolean();
        if (executions && hadRepeatRuns) {
            // repeated executions (Only if had random executions)
            totalRepeatExecutionCount = randomLongBetween(1, totalExecutionCount);
            totalTimeBetweenExecutions = randomLongBetween(1, Long.MAX_VALUE);
            avgTimeBetweenExecutions = totalTimeBetweenExecutions / totalRepeatExecutionCount;
            minTimeBetweenExecutions = randomIntBetween(1, 1000000);
            maxTimeBetweenExecutions = randomLongBetween(minTimeBetweenExecutions, Long.MAX_VALUE);
        } else {
            // no repeated executions
            totalRepeatExecutionCount = 0L;
            totalTimeBetweenExecutions = 0L;
            avgTimeBetweenExecutions = 0L;
            minTimeBetweenExecutions = -1;
            maxTimeBetweenExecutions = 0L;
        }

        return new ExecutionStats(
            totalExecutionCount,
            totalExecutionTime,
            minExecutionTime,
            maxExecutionTime,
            totalRepeatExecutionCount,
            totalTimeBetweenExecutions,
            avgTimeBetweenExecutions,
            minTimeBetweenExecutions,
            maxTimeBetweenExecutions
        );
    }

    public static List<CoordinatorStats> randomCoordinatorStats() {
        int numCoordinatingStats = randomIntBetween(0, 16);
        List<CoordinatorStats> coordinatorStats = new ArrayList<>(numCoordinatingStats);
        for (int i = 0; i < numCoordinatingStats; i++) {
            CoordinatorStats stats = new CoordinatorStats(
                randomAlphaOfLength(4),
                randomIntBetween(0, 8096),
                randomIntBetween(0, 8096),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            coordinatorStats.add(stats);
        }
        return coordinatorStats;
    }

    @Override
    protected Writeable.Reader<EnrichStatsAction.Response> instanceReader() {
        return EnrichStatsAction.Response::new;
    }

    public static TaskInfo randomTaskInfo() {
        TaskId taskId = new TaskId(randomAlphaOfLength(5), randomLong());
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        String description = randomAlphaOfLength(5);
        long startTime = randomLong();
        long runningTimeNanos = randomLong();
        boolean cancellable = randomBoolean();
        TaskId parentTaskId = TaskId.EMPTY_TASK_ID;
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(taskId, type, action, description, null, startTime, runningTimeNanos, cancellable, parentTaskId, headers);
    }
}
