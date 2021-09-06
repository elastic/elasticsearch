/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StatsResponseTests extends AbstractResponseTestCase<EnrichStatsAction.Response, StatsResponse> {

    @Override
    protected EnrichStatsAction.Response createServerTestInstance(XContentType xContentType) {
        int numExecutingPolicies = randomIntBetween(0, 16);
        List<EnrichStatsAction.Response.ExecutingPolicy> executingPolicies = new ArrayList<>(numExecutingPolicies);
        for (int i = 0; i < numExecutingPolicies; i++) {
            TaskInfo taskInfo = randomTaskInfo();
            executingPolicies.add(new EnrichStatsAction.Response.ExecutingPolicy(randomAlphaOfLength(4), taskInfo));
        }
        int numCoordinatingStats = randomIntBetween(0, 16);
        List<EnrichStatsAction.Response.CoordinatorStats> coordinatorStats = new ArrayList<>(numCoordinatingStats);
        List<EnrichStatsAction.Response.CacheStats> cacheStats = new ArrayList<>(numCoordinatingStats);
        for (int i = 0; i < numCoordinatingStats; i++) {
            String nodeId = randomAlphaOfLength(4);
            EnrichStatsAction.Response.CoordinatorStats stats = new EnrichStatsAction.Response.CoordinatorStats(
                nodeId, randomIntBetween(0, 8096), randomIntBetween(0, 8096), randomNonNegativeLong(),
                randomNonNegativeLong());
            coordinatorStats.add(stats);
            cacheStats.add(
                new EnrichStatsAction.Response.CacheStats(nodeId, randomNonNegativeLong(), randomNonNegativeLong(),
                    randomNonNegativeLong(), randomNonNegativeLong())
            );
        }
        return new EnrichStatsAction.Response(executingPolicies, coordinatorStats, cacheStats);
    }

    @Override
    protected StatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return StatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(EnrichStatsAction.Response serverTestInstance, StatsResponse clientInstance) {
        assertThat(clientInstance.getExecutingPolicies().size(), equalTo(serverTestInstance.getExecutingPolicies().size()));
        for (int i = 0; i < clientInstance.getExecutingPolicies().size(); i++) {
            StatsResponse.ExecutingPolicy actual = clientInstance.getExecutingPolicies().get(i);
            EnrichStatsAction.Response.ExecutingPolicy expected = serverTestInstance.getExecutingPolicies().get(i);
            assertThat(actual.getName(), equalTo(expected.getName()));
            assertThat(actual.getTaskInfo(), equalTo(actual.getTaskInfo()));
        }

        assertThat(clientInstance.getCoordinatorStats().size(), equalTo(serverTestInstance.getCoordinatorStats().size()));
        for (int i = 0; i < clientInstance.getCoordinatorStats().size(); i++) {
            StatsResponse.CoordinatorStats actual = clientInstance.getCoordinatorStats().get(i);
            EnrichStatsAction.Response.CoordinatorStats expected = serverTestInstance.getCoordinatorStats().get(i);
            assertThat(actual.getNodeId(), equalTo(expected.getNodeId()));
            assertThat(actual.getQueueSize(), equalTo(expected.getQueueSize()));
            assertThat(actual.getRemoteRequestsCurrent(), equalTo(expected.getRemoteRequestsCurrent()));
            assertThat(actual.getRemoteRequestsTotal(), equalTo(expected.getRemoteRequestsTotal()));
            assertThat(actual.getExecutedSearchesTotal(), equalTo(expected.getExecutedSearchesTotal()));
        }

        assertThat(clientInstance.getCacheStats().size(), equalTo(serverTestInstance.getCacheStats().size()));
        for (int i = 0; i < clientInstance.getCacheStats().size(); i++) {
            StatsResponse.CacheStats actual = clientInstance.getCacheStats().get(i);
            EnrichStatsAction.Response.CacheStats expected = serverTestInstance.getCacheStats().get(i);
            assertThat(actual.getNodeId(), equalTo(expected.getNodeId()));
            assertThat(actual.getCount(), equalTo(expected.getCount()));
            assertThat(actual.getHits(), equalTo(expected.getHits()));
            assertThat(actual.getMisses(), equalTo(expected.getMisses()));
            assertThat(actual.getEvictions(), equalTo(expected.getEvictions()));
        }
    }

    private static TaskInfo randomTaskInfo() {
        TaskId taskId = new TaskId(randomAlphaOfLength(5), randomLong());
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        String description = randomAlphaOfLength(5);
        long startTime = randomLong();
        long runningTimeNanos = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskId parentTaskId = TaskId.EMPTY_TASK_ID;
        Map<String, String> headers = randomBoolean() ?
            Collections.emptyMap() :
            Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
                taskId,
                type,
                action,
                description,
                null,
                startTime,
                runningTimeNanos,
                cancellable,
                cancelled,
                parentTaskId,
                headers);
    }
}
