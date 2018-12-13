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
import org.elasticsearch.client.ccr.IndicesFollowStats.ShardFollowStats;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.ccr.CcrStatsResponseTests.randomIndicesFollowStats;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FollowStatsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            FollowStatsResponseTests::createTestInstance,
            FollowStatsResponseTests::toXContent,
            FollowStatsResponse::fromXContent)
            .supportsUnknownFields(false)
            .assertEqualsConsumer(FollowStatsResponseTests::assertEqualInstances)
            .assertToXContentEquivalence(false)
            .test();
    }

    // Needed, because exceptions in IndicesFollowStats cannot be compared
    private static void assertEqualInstances(FollowStatsResponse expectedInstance, FollowStatsResponse newInstance) {
        assertNotSame(expectedInstance, newInstance);
        {
            IndicesFollowStats newIndicesFollowStats = newInstance.getIndicesFollowStats();
            IndicesFollowStats expectedIndicesFollowStats = expectedInstance.getIndicesFollowStats();
            assertThat(newIndicesFollowStats.getShardFollowStats().size(),
                equalTo(expectedIndicesFollowStats.getShardFollowStats().size()));
            assertThat(newIndicesFollowStats.getShardFollowStats().keySet(),
                equalTo(expectedIndicesFollowStats.getShardFollowStats().keySet()));
            for (Map.Entry<String, List<ShardFollowStats>> indexEntry : newIndicesFollowStats.getShardFollowStats().entrySet()) {
                List<ShardFollowStats> newStats = indexEntry.getValue();
                List<ShardFollowStats> expectedStats = expectedIndicesFollowStats.getShardFollowStats(indexEntry.getKey());
                assertThat(newStats.size(), equalTo(expectedStats.size()));
                for (int i = 0; i < newStats.size(); i++) {
                    ShardFollowStats actualShardFollowStats = newStats.get(i);
                    ShardFollowStats expectedShardFollowStats = expectedStats.get(i);

                    assertThat(actualShardFollowStats.getRemoteCluster(), equalTo(expectedShardFollowStats.getRemoteCluster()));
                    assertThat(actualShardFollowStats.getLeaderIndex(), equalTo(expectedShardFollowStats.getLeaderIndex()));
                    assertThat(actualShardFollowStats.getFollowerIndex(), equalTo(expectedShardFollowStats.getFollowerIndex()));
                    assertThat(actualShardFollowStats.getShardId(), equalTo(expectedShardFollowStats.getShardId()));
                    assertThat(actualShardFollowStats.getLeaderGlobalCheckpoint(),
                        equalTo(expectedShardFollowStats.getLeaderGlobalCheckpoint()));
                    assertThat(actualShardFollowStats.getLeaderMaxSeqNo(), equalTo(expectedShardFollowStats.getLeaderMaxSeqNo()));
                    assertThat(actualShardFollowStats.getFollowerGlobalCheckpoint(),
                        equalTo(expectedShardFollowStats.getFollowerGlobalCheckpoint()));
                    assertThat(actualShardFollowStats.getLastRequestedSeqNo(), equalTo(expectedShardFollowStats.getLastRequestedSeqNo()));
                    assertThat(actualShardFollowStats.getOutstandingReadRequests(),
                        equalTo(expectedShardFollowStats.getOutstandingReadRequests()));
                    assertThat(actualShardFollowStats.getOutstandingWriteRequests(),
                        equalTo(expectedShardFollowStats.getOutstandingWriteRequests()));
                    assertThat(actualShardFollowStats.getWriteBufferOperationCount(),
                        equalTo(expectedShardFollowStats.getWriteBufferOperationCount()));
                    assertThat(actualShardFollowStats.getFollowerMappingVersion(),
                        equalTo(expectedShardFollowStats.getFollowerMappingVersion()));
                    assertThat(actualShardFollowStats.getFollowerSettingsVersion(),
                        equalTo(expectedShardFollowStats.getFollowerSettingsVersion()));
                    assertThat(actualShardFollowStats.getTotalReadTimeMillis(),
                        equalTo(expectedShardFollowStats.getTotalReadTimeMillis()));
                    assertThat(actualShardFollowStats.getSuccessfulReadRequests(),
                        equalTo(expectedShardFollowStats.getSuccessfulReadRequests()));
                    assertThat(actualShardFollowStats.getFailedReadRequests(), equalTo(expectedShardFollowStats.getFailedReadRequests()));
                    assertThat(actualShardFollowStats.getOperationsReads(), equalTo(expectedShardFollowStats.getOperationsReads()));
                    assertThat(actualShardFollowStats.getBytesRead(), equalTo(expectedShardFollowStats.getBytesRead()));
                    assertThat(actualShardFollowStats.getTotalWriteTimeMillis(),
                        equalTo(expectedShardFollowStats.getTotalWriteTimeMillis()));
                    assertThat(actualShardFollowStats.getSuccessfulWriteRequests(),
                        equalTo(expectedShardFollowStats.getSuccessfulWriteRequests()));
                    assertThat(actualShardFollowStats.getFailedWriteRequests(),
                        equalTo(expectedShardFollowStats.getFailedWriteRequests()));
                    assertThat(actualShardFollowStats.getOperationWritten(), equalTo(expectedShardFollowStats.getOperationWritten()));
                    assertThat(actualShardFollowStats.getReadExceptions().size(),
                        equalTo(expectedShardFollowStats.getReadExceptions().size()));
                    assertThat(actualShardFollowStats.getReadExceptions().keySet(),
                        equalTo(expectedShardFollowStats.getReadExceptions().keySet()));
                    for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry :
                        actualShardFollowStats.getReadExceptions().entrySet()) {
                        final Tuple<Integer, ElasticsearchException> expectedTuple =
                            expectedShardFollowStats.getReadExceptions().get(entry.getKey());
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
                        equalTo(expectedShardFollowStats.getTimeSinceLastReadMillis()));
                }
            }
        }
    }

    private static void toXContent(FollowStatsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.startArray(IndicesFollowStats.INDICES_FIELD.getPreferredName());
            for (Map.Entry<String, List<ShardFollowStats>> indexEntry :
                response.getIndicesFollowStats().getShardFollowStats().entrySet()) {
                builder.startObject();
                {
                    builder.field(IndicesFollowStats.INDEX_FIELD.getPreferredName(), indexEntry.getKey());
                    builder.startArray(IndicesFollowStats.SHARDS_FIELD.getPreferredName());
                    {
                        for (ShardFollowStats stats : indexEntry.getValue()) {
                            builder.startObject();
                            {
                                builder.field(ShardFollowStats.LEADER_CLUSTER.getPreferredName(), stats.getRemoteCluster());
                                builder.field(ShardFollowStats.LEADER_INDEX.getPreferredName(), stats.getLeaderIndex());
                                builder.field(ShardFollowStats.FOLLOWER_INDEX.getPreferredName(), stats.getFollowerIndex());
                                builder.field(ShardFollowStats.SHARD_ID.getPreferredName(), stats.getShardId());
                                builder.field(ShardFollowStats.LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(),
                                    stats.getLeaderGlobalCheckpoint());
                                builder.field(ShardFollowStats.LEADER_MAX_SEQ_NO_FIELD.getPreferredName(), stats.getLeaderMaxSeqNo());
                                builder.field(ShardFollowStats.FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(),
                                    stats.getFollowerGlobalCheckpoint());
                                builder.field(ShardFollowStats.FOLLOWER_MAX_SEQ_NO_FIELD.getPreferredName(),
                                    stats.getFollowerMaxSeqNo());
                                builder.field(ShardFollowStats.LAST_REQUESTED_SEQ_NO_FIELD.getPreferredName(),
                                    stats.getLastRequestedSeqNo());
                                builder.field(ShardFollowStats.OUTSTANDING_READ_REQUESTS.getPreferredName(),
                                    stats.getOutstandingReadRequests());
                                builder.field(ShardFollowStats.OUTSTANDING_WRITE_REQUESTS.getPreferredName(),
                                    stats.getOutstandingWriteRequests());
                                builder.field(ShardFollowStats.WRITE_BUFFER_OPERATION_COUNT_FIELD.getPreferredName(),
                                    stats.getWriteBufferOperationCount());
                                builder.humanReadableField(
                                    ShardFollowStats.WRITE_BUFFER_SIZE_IN_BYTES_FIELD.getPreferredName(),
                                    "write_buffer_size",
                                    new ByteSizeValue(stats.getWriteBufferSizeInBytes()));
                                builder.field(ShardFollowStats.FOLLOWER_MAPPING_VERSION_FIELD.getPreferredName(),
                                    stats.getFollowerMappingVersion());
                                builder.field(ShardFollowStats.FOLLOWER_SETTINGS_VERSION_FIELD.getPreferredName(),
                                    stats.getFollowerSettingsVersion());
                                builder.humanReadableField(
                                    ShardFollowStats.TOTAL_READ_TIME_MILLIS_FIELD.getPreferredName(),
                                    "total_read_time",
                                    new TimeValue(stats.getTotalReadTimeMillis(), TimeUnit.MILLISECONDS));
                                builder.humanReadableField(
                                    ShardFollowStats.TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD.getPreferredName(),
                                    "total_read_remote_exec_time",
                                    new TimeValue(stats.getTotalReadRemoteExecTimeMillis(), TimeUnit.MILLISECONDS));
                                builder.field(ShardFollowStats.SUCCESSFUL_READ_REQUESTS_FIELD.getPreferredName(),
                                    stats.getSuccessfulReadRequests());
                                builder.field(ShardFollowStats.FAILED_READ_REQUESTS_FIELD.getPreferredName(),
                                    stats.getFailedReadRequests());
                                builder.field(ShardFollowStats.OPERATIONS_READ_FIELD.getPreferredName(), stats.getOperationsReads());
                                builder.humanReadableField(
                                    ShardFollowStats.BYTES_READ.getPreferredName(),
                                    "total_read",
                                    new ByteSizeValue(stats.getBytesRead(), ByteSizeUnit.BYTES));
                                builder.humanReadableField(
                                    ShardFollowStats.TOTAL_WRITE_TIME_MILLIS_FIELD.getPreferredName(),
                                    "total_write_time",
                                    new TimeValue(stats.getTotalWriteTimeMillis(), TimeUnit.MILLISECONDS));
                                builder.field(ShardFollowStats.SUCCESSFUL_WRITE_REQUESTS_FIELD.getPreferredName(),
                                    stats.getSuccessfulWriteRequests());
                                builder.field(ShardFollowStats.FAILED_WRITE_REQUEST_FIELD.getPreferredName(),
                                    stats.getFailedWriteRequests());
                                builder.field(ShardFollowStats.OPERATIONS_WRITTEN.getPreferredName(), stats.getOperationWritten());
                                builder.startArray(ShardFollowStats.READ_EXCEPTIONS.getPreferredName());
                                {
                                    for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry :
                                        stats.getReadExceptions().entrySet()) {
                                        builder.startObject();
                                        {
                                            builder.field(ShardFollowStats.READ_EXCEPTIONS_ENTRY_FROM_SEQ_NO.getPreferredName(),
                                                entry.getKey());
                                            builder.field(ShardFollowStats.READ_EXCEPTIONS_RETRIES.getPreferredName(),
                                                entry.getValue().v1());
                                            builder.field(ShardFollowStats.READ_EXCEPTIONS_ENTRY_EXCEPTION.getPreferredName());
                                            builder.startObject();
                                            {
                                                ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS,
                                                    entry.getValue().v2());
                                            }
                                            builder.endObject();
                                        }
                                        builder.endObject();
                                    }
                                }
                                builder.endArray();
                                builder.humanReadableField(
                                    ShardFollowStats.TIME_SINCE_LAST_READ_MILLIS_FIELD.getPreferredName(),
                                    "time_since_last_read",
                                    new TimeValue(stats.getTimeSinceLastReadMillis(), TimeUnit.MILLISECONDS));
                                if (stats.getFatalException() != null) {
                                    builder.field(ShardFollowStats.FATAL_EXCEPTION.getPreferredName());
                                    builder.startObject();
                                    {
                                        ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS,
                                            stats.getFatalException());
                                    }
                                    builder.endObject();
                                }
                            }
                            builder.endObject();
                        }
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
    }

    private static FollowStatsResponse createTestInstance() {
        return new FollowStatsResponse(randomIndicesFollowStats());
    }

}
