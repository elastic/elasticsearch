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
        IndicesFollowStats newIndicesFollowStats = clientInstance.IndicesFollowStats();

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
                ShardFollowNodeTaskStatus expectedShardFollowStats = expectedStats.get(actualShardFollowStats.ShardId()).status();

                assertThat(actualShardFollowStats.RemoteCluster(), equalTo(expectedShardFollowStats.getRemoteCluster()));
                assertThat(actualShardFollowStats.LeaderIndex(), equalTo(expectedShardFollowStats.leaderIndex()));
                assertThat(actualShardFollowStats.FollowerIndex(), equalTo(expectedShardFollowStats.followerIndex()));
                assertThat(actualShardFollowStats.ShardId(), equalTo(expectedShardFollowStats.getShardId()));
                assertThat(actualShardFollowStats.LeaderGlobalCheckpoint(),
                    equalTo(expectedShardFollowStats.leaderGlobalCheckpoint()));
                assertThat(actualShardFollowStats.LeaderMaxSeqNo(), equalTo(expectedShardFollowStats.leaderMaxSeqNo()));
                assertThat(actualShardFollowStats.FollowerGlobalCheckpoint(),
                    equalTo(expectedShardFollowStats.followerGlobalCheckpoint()));
                assertThat(actualShardFollowStats.LastRequestedSeqNo(), equalTo(expectedShardFollowStats.lastRequestedSeqNo()));
                assertThat(actualShardFollowStats.OutstandingReadRequests(),
                    equalTo(expectedShardFollowStats.outstandingReadRequests()));
                assertThat(actualShardFollowStats.OutstandingWriteRequests(),
                    equalTo(expectedShardFollowStats.outstandingWriteRequests()));
                assertThat(actualShardFollowStats.WriteBufferOperationCount(),
                    equalTo(expectedShardFollowStats.writeBufferOperationCount()));
                assertThat(actualShardFollowStats.FollowerMappingVersion(),
                    equalTo(expectedShardFollowStats.followerMappingVersion()));
                assertThat(actualShardFollowStats.FollowerSettingsVersion(),
                    equalTo(expectedShardFollowStats.followerSettingsVersion()));
                assertThat(actualShardFollowStats.FollowerAliasesVersion(),
                        equalTo(expectedShardFollowStats.followerAliasesVersion()));
                assertThat(actualShardFollowStats.TotalReadTimeMillis(),
                    equalTo(expectedShardFollowStats.totalReadTimeMillis()));
                assertThat(actualShardFollowStats.SuccessfulReadRequests(),
                    equalTo(expectedShardFollowStats.successfulReadRequests()));
                assertThat(actualShardFollowStats.FailedReadRequests(), equalTo(expectedShardFollowStats.failedReadRequests()));
                assertThat(actualShardFollowStats.OperationsReads(), equalTo(expectedShardFollowStats.operationsReads()));
                assertThat(actualShardFollowStats.BytesRead(), equalTo(expectedShardFollowStats.bytesRead()));
                assertThat(actualShardFollowStats.TotalWriteTimeMillis(),
                    equalTo(expectedShardFollowStats.totalWriteTimeMillis()));
                assertThat(actualShardFollowStats.SuccessfulWriteRequests(),
                    equalTo(expectedShardFollowStats.successfulWriteRequests()));
                assertThat(actualShardFollowStats.FailedWriteRequests(),
                    equalTo(expectedShardFollowStats.failedWriteRequests()));
                assertThat(actualShardFollowStats.OperationWritten(), equalTo(expectedShardFollowStats.operationWritten()));
                assertThat(actualShardFollowStats.ReadExceptions().size(),
                    equalTo(expectedShardFollowStats.readExceptions().size()));
                assertThat(actualShardFollowStats.ReadExceptions().keySet(),
                    equalTo(expectedShardFollowStats.readExceptions().keySet()));
                for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry :
                    actualShardFollowStats.ReadExceptions().entrySet()) {
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
                assertThat(actualShardFollowStats.TimeSinceLastReadMillis(),
                    equalTo(expectedShardFollowStats.timeSinceLastReadMillis()));
            }
        }
    }

}
