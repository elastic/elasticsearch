/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CacheStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;

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
        int numCoordinatingStats = randomIntBetween(0, 16);
        List<CoordinatorStats> coordinatorStats = new ArrayList<>(numCoordinatingStats);
        List<CacheStats> cacheStats = new ArrayList<>(numCoordinatingStats);
        for (int i = 0; i < numCoordinatingStats; i++) {
            String nodeId = randomAlphaOfLength(4);
            CoordinatorStats stats = new CoordinatorStats(
                nodeId,
                randomIntBetween(0, 8096),
                randomIntBetween(0, 8096),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            coordinatorStats.add(stats);
            cacheStats.add(
                new CacheStats(
                    nodeId,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
        }
        return new EnrichStatsAction.Response(executingPolicies, coordinatorStats, cacheStats);
    }

    @Override
    protected EnrichStatsAction.Response mutateInstance(EnrichStatsAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<EnrichStatsAction.Response> instanceReader() {
        return EnrichStatsAction.Response::new;
    }

    public static TaskInfo randomTaskInfo() {
        String nodeId = randomAlphaOfLength(5);
        TaskId taskId = new TaskId(nodeId, randomLong());
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        String description = randomAlphaOfLength(5);
        long startTime = randomLong();
        long runningTimeNanos = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskId parentTaskId = TaskId.EMPTY_TASK_ID;
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
            taskId,
            type,
            nodeId,
            action,
            description,
            null,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers
        );
    }
}
