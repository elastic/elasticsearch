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
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CcrStatsResponseTests extends AbstractResponseTestCase<CcrStatsAction.Response, CcrStatsResponse> {

    @Override
    protected CcrStatsAction.Response createServerTestInstance(XContentType xContentType) {
        org.elasticsearch.xpack.core.ccr.AutoFollowStats autoFollowStats = new org.elasticsearch.xpack.core.ccr.AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomReadExceptions(),
            randomTrackingClusters()
        );
        FollowStatsAction.StatsResponses statsResponse = createStatsResponse();
        return new CcrStatsAction.Response(autoFollowStats, statsResponse);
    }

    static NavigableMap<String, Tuple<Long, ElasticsearchException>> randomReadExceptions() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<String, Tuple<Long, ElasticsearchException>> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put("" + i, Tuple.tuple(randomNonNegativeLong(),
                new ElasticsearchException(new IllegalStateException("index [" + i + "]"))));
        }
        return readExceptions;
    }

    static NavigableMap<String, org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster> randomTrackingClusters() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<String, org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put("" + i,
                new org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster(randomLong(), randomNonNegativeLong()));
        }
        return readExceptions;
    }

    static FollowStatsAction.StatsResponses createStatsResponse() {
        int numResponses = randomIntBetween(0, 8);
        List<FollowStatsAction.StatsResponse> responses = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Collections.emptyNavigableMap(),
                randomNonNegativeLong(),
                randomBoolean() ? new ElasticsearchException("fatal error") : null);
            responses.add(new FollowStatsAction.StatsResponse(status));
        }
        return new FollowStatsAction.StatsResponses(Collections.emptyList(), Collections.emptyList(), responses);
    }

    @Override
    protected CcrStatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return CcrStatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(CcrStatsAction.Response serverTestInstance, CcrStatsResponse clientInstance) {
        {
            AutoFollowStats newAutoFollowStats = clientInstance.AutoFollowStats();
            org.elasticsearch.xpack.core.ccr.AutoFollowStats expectedAutoFollowStats = serverTestInstance.getAutoFollowStats();
            assertThat(newAutoFollowStats.getNumberOfSuccessfulFollowIndices(),
                equalTo(expectedAutoFollowStats.NumberOfSuccessfulFollowIndices()));
            assertThat(newAutoFollowStats.getNumberOfFailedRemoteClusterStateRequests(),
                equalTo(expectedAutoFollowStats.NumberOfFailedRemoteClusterStateRequests()));
            assertThat(newAutoFollowStats.getNumberOfFailedFollowIndices(),
                equalTo(expectedAutoFollowStats.NumberOfFailedFollowIndices()));
            assertThat(newAutoFollowStats.getRecentAutoFollowErrors().size(),
                equalTo(expectedAutoFollowStats.RecentAutoFollowErrors().size()));
            assertThat(newAutoFollowStats.getRecentAutoFollowErrors().keySet(),
                equalTo(expectedAutoFollowStats.RecentAutoFollowErrors().keySet()));
            for (final Map.Entry<String, Tuple<Long, ElasticsearchException>> entry :
                newAutoFollowStats.getRecentAutoFollowErrors().entrySet()) {
                // x-content loses the exception
                final Tuple<Long, ElasticsearchException> expected =
                    expectedAutoFollowStats.RecentAutoFollowErrors().get(entry.getKey());
                assertThat(entry.getValue().v2().getMessage(), containsString(expected.v2().getMessage()));
                assertThat(entry.getValue().v1(), equalTo(expected.v1()));
                assertNotNull(entry.getValue().v2().getCause());
                assertThat(
                    entry.getValue().v2().getCause(),
                    anyOf(instanceOf(ElasticsearchException.class), instanceOf(IllegalStateException.class)));
                assertThat(entry.getValue().v2().getCause().getMessage(), containsString(expected.v2().getCause().getMessage()));
            }
        }
        {
            IndicesFollowStats newIndicesFollowStats = clientInstance.IndicesFollowStats();

            // sort by index name, then shard ID
            final Map<String, Map<Integer, FollowStatsAction.StatsResponse>> expectedIndicesFollowStats = new TreeMap<>();
            for (final FollowStatsAction.StatsResponse statsResponse : serverTestInstance.getFollowStats().getStatsResponses()) {
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

}
