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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class IndicesFollowStats {

    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField INDEX_FIELD = new ParseField("index");
    static final ParseField SHARDS_FIELD = new ParseField("shards");

    private static final ConstructingObjectParser<Tuple<String, List<ShardFollowStats>>, Void> ENTRY_PARSER =
        new ConstructingObjectParser<>(
            "entry",
            true,
            args -> {
                String index = (String) args[0];
                @SuppressWarnings("unchecked")
                List<ShardFollowStats> shardFollowStats = (List<ShardFollowStats>) args[1];
                return new Tuple<>(index, shardFollowStats);
            }
        );

    static {
        ENTRY_PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        ENTRY_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), ShardFollowStats.PARSER, SHARDS_FIELD);
    }

    static final ConstructingObjectParser<IndicesFollowStats, Void> PARSER = new ConstructingObjectParser<>(
        "indices",
        true,
        args -> {
            @SuppressWarnings("unchecked")
            List<Tuple<String, List<ShardFollowStats>>> entries = (List<Tuple<String, List<ShardFollowStats>>>) args[0];
            Map<String, List<ShardFollowStats>> shardFollowStats = entries.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            return new IndicesFollowStats(new TreeMap<>(shardFollowStats));
        });

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), ENTRY_PARSER, INDICES_FIELD);
    }

    private final NavigableMap<String, List<ShardFollowStats>> shardFollowStats;

    IndicesFollowStats(NavigableMap<String, List<ShardFollowStats>> shardFollowStats) {
        this.shardFollowStats = Collections.unmodifiableNavigableMap(shardFollowStats);
    }

    public List<ShardFollowStats> getShardFollowStats(String index) {
        return shardFollowStats.get(index);
    }

    public Map<String, List<ShardFollowStats>> getShardFollowStats() {
        return shardFollowStats;
    }

    public static final class ShardFollowStats {


        static final ParseField LEADER_CLUSTER = new ParseField("remote_cluster");
        static final ParseField LEADER_INDEX = new ParseField("leader_index");
        static final ParseField FOLLOWER_INDEX = new ParseField("follower_index");
        static final ParseField SHARD_ID = new ParseField("shard_id");
        static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
        static final ParseField LEADER_MAX_SEQ_NO_FIELD = new ParseField("leader_max_seq_no");
        static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");
        static final ParseField FOLLOWER_MAX_SEQ_NO_FIELD = new ParseField("follower_max_seq_no");
        static final ParseField LAST_REQUESTED_SEQ_NO_FIELD = new ParseField("last_requested_seq_no");
        static final ParseField OUTSTANDING_READ_REQUESTS = new ParseField("outstanding_read_requests");
        static final ParseField OUTSTANDING_WRITE_REQUESTS = new ParseField("outstanding_write_requests");
        static final ParseField WRITE_BUFFER_OPERATION_COUNT_FIELD = new ParseField("write_buffer_operation_count");
        static final ParseField WRITE_BUFFER_SIZE_IN_BYTES_FIELD = new ParseField("write_buffer_size_in_bytes");
        static final ParseField FOLLOWER_MAPPING_VERSION_FIELD = new ParseField("follower_mapping_version");
        static final ParseField FOLLOWER_SETTINGS_VERSION_FIELD = new ParseField("follower_settings_version");
        static final ParseField FOLLOWER_ALIASES_VERSION_FIELD = new ParseField("follower_aliases_version");
        static final ParseField TOTAL_READ_TIME_MILLIS_FIELD = new ParseField("total_read_time_millis");
        static final ParseField TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD = new ParseField("total_read_remote_exec_time_millis");
        static final ParseField SUCCESSFUL_READ_REQUESTS_FIELD = new ParseField("successful_read_requests");
        static final ParseField FAILED_READ_REQUESTS_FIELD = new ParseField("failed_read_requests");
        static final ParseField OPERATIONS_READ_FIELD = new ParseField("operations_read");
        static final ParseField BYTES_READ = new ParseField("bytes_read");
        static final ParseField TOTAL_WRITE_TIME_MILLIS_FIELD = new ParseField("total_write_time_millis");
        static final ParseField SUCCESSFUL_WRITE_REQUESTS_FIELD = new ParseField("successful_write_requests");
        static final ParseField FAILED_WRITE_REQUEST_FIELD = new ParseField("failed_write_requests");
        static final ParseField OPERATIONS_WRITTEN = new ParseField("operations_written");
        static final ParseField READ_EXCEPTIONS = new ParseField("read_exceptions");
        static final ParseField TIME_SINCE_LAST_READ_MILLIS_FIELD = new ParseField("time_since_last_read_millis");
        static final ParseField FATAL_EXCEPTION = new ParseField("fatal_exception");

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<ShardFollowStats, Void> PARSER =
                new ConstructingObjectParser<>(
                        "shard-follow-stats",
                        true,
                        args -> new ShardFollowStats(
                                (String) args[0],
                                (String) args[1],
                                (String) args[2],
                                (int) args[3],
                                (long) args[4],
                                (long) args[5],
                                (long) args[6],
                                (long) args[7],
                                (long) args[8],
                                (int) args[9],
                                (int) args[10],
                                (int) args[11],
                                (long) args[12],
                                (long) args[13],
                                (long) args[14],
                                (long) args[15],
                                (long) args[16],
                                (long) args[17],
                                (long) args[18],
                                (long) args[19],
                                (long) args[20],
                                (long) args[21],
                                (long) args[22],
                                (long) args[23],
                                (long) args[24],
                                (long) args[25],
                                (long) args[26],
                                new TreeMap<>(
                                        ((List<Map.Entry<Long, Tuple<Integer, ElasticsearchException>>>) args[27])
                                                .stream()
                                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
                                (ElasticsearchException) args[28]));

        static final ConstructingObjectParser<Map.Entry<Long, Tuple<Integer, ElasticsearchException>>, Void> READ_EXCEPTIONS_ENTRY_PARSER =
            new ConstructingObjectParser<>(
                "shard-follow-stats-read-exceptions-entry",
                true,
                args -> new AbstractMap.SimpleEntry<>((long) args[0], Tuple.tuple((Integer) args[1], (ElasticsearchException)args[2])));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_CLUSTER);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOWER_INDEX);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_MAX_SEQ_NO_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAX_SEQ_NO_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_REQUESTED_SEQ_NO_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), WRITE_BUFFER_OPERATION_COUNT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), WRITE_BUFFER_SIZE_IN_BYTES_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAPPING_VERSION_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_SETTINGS_VERSION_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_ALIASES_VERSION_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_READ_TIME_MILLIS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SUCCESSFUL_READ_REQUESTS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILED_READ_REQUESTS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_READ_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), BYTES_READ);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_WRITE_TIME_MILLIS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SUCCESSFUL_WRITE_REQUESTS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILED_WRITE_REQUEST_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_WRITTEN);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_SINCE_LAST_READ_MILLIS_FIELD);
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), READ_EXCEPTIONS_ENTRY_PARSER, READ_EXCEPTIONS);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ElasticsearchException.fromXContent(p),
                FATAL_EXCEPTION);
        }

        static final ParseField READ_EXCEPTIONS_ENTRY_FROM_SEQ_NO = new ParseField("from_seq_no");
        static final ParseField READ_EXCEPTIONS_RETRIES = new ParseField("retries");
        static final ParseField READ_EXCEPTIONS_ENTRY_EXCEPTION = new ParseField("exception");

        static {
            READ_EXCEPTIONS_ENTRY_PARSER.declareLong(ConstructingObjectParser.constructorArg(), READ_EXCEPTIONS_ENTRY_FROM_SEQ_NO);
            READ_EXCEPTIONS_ENTRY_PARSER.declareInt(ConstructingObjectParser.constructorArg(), READ_EXCEPTIONS_RETRIES);
            READ_EXCEPTIONS_ENTRY_PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ElasticsearchException.fromXContent(p),
                READ_EXCEPTIONS_ENTRY_EXCEPTION);
        }

        private final String remoteCluster;
        private final String leaderIndex;
        private final String followerIndex;
        private final int shardId;
        private final long leaderGlobalCheckpoint;
        private final long leaderMaxSeqNo;
        private final long followerGlobalCheckpoint;
        private final long followerMaxSeqNo;
        private final long lastRequestedSeqNo;
        private final int outstandingReadRequests;
        private final int outstandingWriteRequests;
        private final int writeBufferOperationCount;
        private final long writeBufferSizeInBytes;
        private final long followerMappingVersion;
        private final long followerSettingsVersion;
        private final long followerAliasesVersion;
        private final long totalReadTimeMillis;
        private final long totalReadRemoteExecTimeMillis;
        private final long successfulReadRequests;
        private final long failedReadRequests;
        private final long operationsReads;
        private final long bytesRead;
        private final long totalWriteTimeMillis;
        private final long successfulWriteRequests;
        private final long failedWriteRequests;
        private final long operationWritten;
        private final long timeSinceLastReadMillis;
        private final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions;
        private final ElasticsearchException fatalException;

        ShardFollowStats(String remoteCluster,
                         String leaderIndex,
                         String followerIndex,
                         int shardId,
                         long leaderGlobalCheckpoint,
                         long leaderMaxSeqNo,
                         long followerGlobalCheckpoint,
                         long followerMaxSeqNo,
                         long lastRequestedSeqNo,
                         int outstandingReadRequests,
                         int outstandingWriteRequests,
                         int writeBufferOperationCount,
                         long writeBufferSizeInBytes,
                         long followerMappingVersion,
                         long followerSettingsVersion,
                         long followerAliasesVersion,
                         long totalReadTimeMillis,
                         long totalReadRemoteExecTimeMillis,
                         long successfulReadRequests,
                         long failedReadRequests,
                         long operationsReads,
                         long bytesRead,
                         long totalWriteTimeMillis,
                         long successfulWriteRequests,
                         long failedWriteRequests,
                         long operationWritten,
                         long timeSinceLastReadMillis,
                         NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions,
                         ElasticsearchException fatalException) {
            this.remoteCluster = remoteCluster;
            this.leaderIndex = leaderIndex;
            this.followerIndex = followerIndex;
            this.shardId = shardId;
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.leaderMaxSeqNo = leaderMaxSeqNo;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.followerMaxSeqNo = followerMaxSeqNo;
            this.lastRequestedSeqNo = lastRequestedSeqNo;
            this.outstandingReadRequests = outstandingReadRequests;
            this.outstandingWriteRequests = outstandingWriteRequests;
            this.writeBufferOperationCount = writeBufferOperationCount;
            this.writeBufferSizeInBytes = writeBufferSizeInBytes;
            this.followerMappingVersion = followerMappingVersion;
            this.followerSettingsVersion = followerSettingsVersion;
            this.followerAliasesVersion = followerAliasesVersion;
            this.totalReadTimeMillis = totalReadTimeMillis;
            this.totalReadRemoteExecTimeMillis = totalReadRemoteExecTimeMillis;
            this.successfulReadRequests = successfulReadRequests;
            this.failedReadRequests = failedReadRequests;
            this.operationsReads = operationsReads;
            this.bytesRead = bytesRead;
            this.totalWriteTimeMillis = totalWriteTimeMillis;
            this.successfulWriteRequests = successfulWriteRequests;
            this.failedWriteRequests = failedWriteRequests;
            this.operationWritten = operationWritten;
            this.timeSinceLastReadMillis = timeSinceLastReadMillis;
            this.readExceptions = readExceptions;
            this.fatalException = fatalException;
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public String getFollowerIndex() {
            return followerIndex;
        }

        public int getShardId() {
            return shardId;
        }

        public long getLeaderGlobalCheckpoint() {
            return leaderGlobalCheckpoint;
        }

        public long getLeaderMaxSeqNo() {
            return leaderMaxSeqNo;
        }

        public long getFollowerGlobalCheckpoint() {
            return followerGlobalCheckpoint;
        }

        public long getFollowerMaxSeqNo() {
            return followerMaxSeqNo;
        }

        public long getLastRequestedSeqNo() {
            return lastRequestedSeqNo;
        }

        public int getOutstandingReadRequests() {
            return outstandingReadRequests;
        }

        public int getOutstandingWriteRequests() {
            return outstandingWriteRequests;
        }

        public int getWriteBufferOperationCount() {
            return writeBufferOperationCount;
        }

        public long getWriteBufferSizeInBytes() {
            return writeBufferSizeInBytes;
        }

        public long getFollowerMappingVersion() {
            return followerMappingVersion;
        }

        public long getFollowerSettingsVersion() {
            return followerSettingsVersion;
        }

        public long getFollowerAliasesVersion() {
            return followerAliasesVersion;
        }

        public long getTotalReadTimeMillis() {
            return totalReadTimeMillis;
        }

        public long getTotalReadRemoteExecTimeMillis() {
            return totalReadRemoteExecTimeMillis;
        }

        public long getSuccessfulReadRequests() {
            return successfulReadRequests;
        }

        public long getFailedReadRequests() {
            return failedReadRequests;
        }

        public long getOperationsReads() {
            return operationsReads;
        }

        public long getBytesRead() {
            return bytesRead;
        }

        public long getTotalWriteTimeMillis() {
            return totalWriteTimeMillis;
        }

        public long getSuccessfulWriteRequests() {
            return successfulWriteRequests;
        }

        public long getFailedWriteRequests() {
            return failedWriteRequests;
        }

        public long getOperationWritten() {
            return operationWritten;
        }

        public long getTimeSinceLastReadMillis() {
            return timeSinceLastReadMillis;
        }

        public NavigableMap<Long, Tuple<Integer, ElasticsearchException>> getReadExceptions() {
            return readExceptions;
        }

        public ElasticsearchException getFatalException() {
            return fatalException;
        }
    }

}
