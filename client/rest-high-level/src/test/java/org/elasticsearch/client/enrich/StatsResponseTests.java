/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        for (int i = 0; i < numCoordinatingStats; i++) {
            EnrichStatsAction.Response.CoordinatorStats stats = new EnrichStatsAction.Response.CoordinatorStats(
                randomAlphaOfLength(4), randomIntBetween(0, 8096), randomIntBetween(0, 8096), randomNonNegativeLong(),
                randomNonNegativeLong());
            coordinatorStats.add(stats);
        }
        return new EnrichStatsAction.Response(executingPolicies, coordinatorStats);
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
    }

    private static TaskInfo randomTaskInfo() {
        TaskId taskId = new TaskId(randomAlphaOfLength(5), randomLong());
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        String description = randomAlphaOfLength(5);
        long startTime = randomLong();
        long runningTimeNanos = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        TaskId parentTaskId = TaskId.EMPTY_TASK_ID;
        Map<String, String> headers = randomBoolean() ?
            Collections.emptyMap() :
            Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(taskId, type, action, description, null, startTime, runningTimeNanos, cancellable, parentTaskId, headers);
    }
}
