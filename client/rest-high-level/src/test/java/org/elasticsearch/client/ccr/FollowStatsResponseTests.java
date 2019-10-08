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

package org.elasticsearch.client.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.ccr.IndicesFollowStats.ShardFollowStats;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.client.ccr.CcrStatsResponseTests.createStatsResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FollowStatsResponseTests extends AbstractResponseTestCase<FollowStatsAction.StatsResponses, FollowStatsResponse> {

    @Override
    protected FollowStatsAction.StatsResponses createServerTestInstance(XContentType xContentType) {
        return createStatsResponse();
    }

    @Override
    protected FollowStatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return FollowStatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(FollowStatsAction.StatsResponses serverTestInstance, FollowStatsResponse clientInstance) {
        IndicesFollowStats newIndicesFollowStats = clientInstance.getIndicesFollowStats();

        // sort by index name, then shard ID
        final Map<String, Map<Integer, FollowStatsAction.StatsResponse>> expectedIndicesFollowStats = new TreeMap<>();
        for (final FollowStatsAction.StatsResponse statsResponse : serverTestInstance.getStatsResponses()) {
            expectedIndicesFollowStats.computeIfAbsent(
                statsResponse.status().followerIndex(),
                k -> new TreeMap<>()).put(statsResponse.status().getShardId(), statsResponse);
        }
        assertThat(newIndicesFollowStats.getShardFollowStats().size(),
            equalTo(expectedIndicesFollowStats.size()));
        assertThat(newIndicesFollowStats.getShardFollowStats().keySet(),
            equalTo(expectedIndicesFollowStats.keySet()));
        for (Map.Entry<String, List<ShardFollowStats>> indexEntry : newIndicesFollowStats.getShardFollowStats().entrySet()) {
            List<ShardFollowStats> newStats = indexEntry.getValue();
            Map<Integer, FollowStatsAction.StatsResponse> expectedStats = expectedIndicesFollowStats.get(indexEntry.getKey());
            assertThat(newStats.size(), equalTo(expectedStats.size()));
            for (int i = 0; i < newStats.size(); i++) {
                ShardFollowStats actualShardFollowStats = newStats.get(i);
                ShardFollowNodeTaskStatus expectedShardFollowStats = expectedStats.get(actualShardFollowStats.getShardId()).status();

                assertThat(actualShardFollowStats.getRemoteCluster(), equalTo(expectedShardFollowStats.getRemoteCluster()));
                assertThat(actualShardFollowStats.getLeaderIndex(), equalTo(expectedShardFollowStats.leaderIndex()));
                assertThat(actualShardFollowStats.getFollowerIndex(), equalTo(expectedShardFollowStats.followerIndex()));
                assertThat(actualShardFollowStats.getShardId(), equalTo(expectedShardFollowStats.getShardId()));
                assertThat(actualShardFollowStats.getLeaderGlobalCheckpoint(),
                    equalTo(expectedShardFollowStats.leaderGlobalCheckpoint()));
                assertThat(actualShardFollowStats.getLeaderMaxSeqNo(), equalTo(expectedShardFollowStats.leaderMaxSeqNo()));
                assertThat(actualShardFollowStats.getFollowerGlobalCheckpoint(),
                    equalTo(expectedShardFollowStats.followerGlobalCheckpoint()));
                assertThat(actualShardFollowStats.getLastRequestedSeqNo(), equalTo(expectedShardFollowStats.lastRequestedSeqNo()));
                assertThat(actualShardFollowStats.getOutstandingReadRequests(),
                    equalTo(expectedShardFollowStats.outstandingReadRequests()));
                assertThat(actualShardFollowStats.getOutstandingWriteRequests(),
                    equalTo(expectedShardFollowStats.outstandingWriteRequests()));
                assertThat(actualShardFollowStats.getWriteBufferOperationCount(),
                    equalTo(expectedShardFollowStats.writeBufferOperationCount()));
                assertThat(actualShardFollowStats.getFollowerMappingVersion(),
                    equalTo(expectedShardFollowStats.followerMappingVersion()));
                assertThat(actualShardFollowStats.getFollowerSettingsVersion(),
                    equalTo(expectedShardFollowStats.followerSettingsVersion()));
                assertThat(actualShardFollowStats.getFollowerAliasesVersion(),
                        equalTo(expectedShardFollowStats.followerAliasesVersion()));
                assertThat(actualShardFollowStats.getTotalReadTimeMillis(),
                    equalTo(expectedShardFollowStats.totalReadTimeMillis()));
                assertThat(actualShardFollowStats.getSuccessfulReadRequests(),
                    equalTo(expectedShardFollowStats.successfulReadRequests()));
                assertThat(actualShardFollowStats.getFailedReadRequests(), equalTo(expectedShardFollowStats.failedReadRequests()));
                assertThat(actualShardFollowStats.getOperationsReads(), equalTo(expectedShardFollowStats.operationsReads()));
                assertThat(actualShardFollowStats.getBytesRead(), equalTo(expectedShardFollowStats.bytesRead()));
                assertThat(actualShardFollowStats.getTotalWriteTimeMillis(),
                    equalTo(expectedShardFollowStats.totalWriteTimeMillis()));
                assertThat(actualShardFollowStats.getSuccessfulWriteRequests(),
                    equalTo(expectedShardFollowStats.successfulWriteRequests()));
                assertThat(actualShardFollowStats.getFailedWriteRequests(),
                    equalTo(expectedShardFollowStats.failedWriteRequests()));
                assertThat(actualShardFollowStats.getOperationWritten(), equalTo(expectedShardFollowStats.operationWritten()));
                assertThat(actualShardFollowStats.getReadExceptions().size(),
                    equalTo(expectedShardFollowStats.readExceptions().size()));
                assertThat(actualShardFollowStats.getReadExceptions().keySet(),
                    equalTo(expectedShardFollowStats.readExceptions().keySet()));
                for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry :
                    actualShardFollowStats.getReadExceptions().entrySet()) {
                    final Tuple<Integer, ElasticsearchException> expectedTuple =
                        expectedShardFollowStats.readExceptions().get(entry.getKey());
                    assertThat(entry.getValue().v1(), equalTo(expectedTuple.v1()));
                    // x-content loses the exception
                    final ElasticsearchException expected = expectedTuple.v2();
                    assertThat(entry.getValue().v2().getMessage(), containsString(expected.getMessage()));
                    assertNotNull(entry.getValue().v2().getCause());
                    assertThat(
                        entry.getValue().v2().getCause(),
                        anyOf(instanceOf(ElasticsearchException.class), instanceOf(IllegalStateException.class)));
                    assertThat(entry.getValue().v2().getCause().getMessage(), containsString(expected.getCause().getMessage()));
                }
                assertThat(actualShardFollowStats.getTimeSinceLastReadMillis(),
                    equalTo(expectedShardFollowStats.timeSinceLastReadMillis()));
            }
        }
    }

}
