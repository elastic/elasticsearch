/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ShardFollowNodeTaskStatus implements Task.Status {

    public static final String STATUS_PARSER_NAME = "shard-follow-node-task-status";

    private static final ParseField LEADER_CLUSTER = new ParseField("remote_cluster");
    private static final ParseField LEADER_INDEX = new ParseField("leader_index");
    private static final ParseField FOLLOWER_INDEX = new ParseField("follower_index");
    private static final ParseField SHARD_ID = new ParseField("shard_id");
    private static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
    private static final ParseField LEADER_MAX_SEQ_NO_FIELD = new ParseField("leader_max_seq_no");
    private static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");
    private static final ParseField FOLLOWER_MAX_SEQ_NO_FIELD = new ParseField("follower_max_seq_no");
    private static final ParseField LAST_REQUESTED_SEQ_NO_FIELD = new ParseField("last_requested_seq_no");
    private static final ParseField OUTSTANDING_READ_REQUESTS = new ParseField("outstanding_read_requests");
    private static final ParseField OUTSTANDING_WRITE_REQUESTS = new ParseField("outstanding_write_requests");
    private static final ParseField WRITE_BUFFER_OPERATION_COUNT_FIELD = new ParseField("write_buffer_operation_count");
    private static final ParseField WRITE_BUFFER_SIZE_IN_BYTES_FIELD = new ParseField("write_buffer_size_in_bytes");
    private static final ParseField FOLLOWER_MAPPING_VERSION_FIELD = new ParseField("follower_mapping_version");
    private static final ParseField FOLLOWER_SETTINGS_VERSION_FIELD = new ParseField("follower_settings_version");
    private static final ParseField FOLLOWER_ALIASES_VERSION_FIELD = new ParseField("follower_aliases_version");
    private static final ParseField TOTAL_READ_TIME_MILLIS_FIELD = new ParseField("total_read_time_millis");
    private static final ParseField TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD = new ParseField("total_read_remote_exec_time_millis");
    private static final ParseField SUCCESSFUL_READ_REQUESTS_FIELD = new ParseField("successful_read_requests");
    private static final ParseField FAILED_READ_REQUESTS_FIELD = new ParseField("failed_read_requests");
    private static final ParseField OPERATIONS_READ_FIELD = new ParseField("operations_read");
    private static final ParseField BYTES_READ = new ParseField("bytes_read");
    private static final ParseField TOTAL_WRITE_TIME_MILLIS_FIELD = new ParseField("total_write_time_millis");
    private static final ParseField SUCCESSFUL_WRITE_REQUESTS_FIELD = new ParseField("successful_write_requests");
    private static final ParseField FAILED_WRITE_REQUEST_FIELD = new ParseField("failed_write_requests");
    private static final ParseField OPERATIONS_WRITTEN = new ParseField("operations_written");
    private static final ParseField READ_EXCEPTIONS = new ParseField("read_exceptions");
    private static final ParseField TIME_SINCE_LAST_READ_MILLIS_FIELD = new ParseField("time_since_last_read_millis");
    private static final ParseField FATAL_EXCEPTION = new ParseField("fatal_exception");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ShardFollowNodeTaskStatus, Void> STATUS_PARSER =
            new ConstructingObjectParser<>(
                    STATUS_PARSER_NAME,
                    args -> new ShardFollowNodeTaskStatus(
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
                            new TreeMap<>(
                                    ((List<Map.Entry<Long, Tuple<Integer, ElasticsearchException>>>) args[26])
                                            .stream()
                                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
                            (long) args[27],
                            (ElasticsearchException) args[28]));

    public static final String READ_EXCEPTIONS_ENTRY_PARSER_NAME = "shard-follow-node-task-status-read-exceptions-entry";

    static final ConstructingObjectParser<Map.Entry<Long, Tuple<Integer, ElasticsearchException>>, Void> READ_EXCEPTIONS_ENTRY_PARSER =
            new ConstructingObjectParser<>(
                READ_EXCEPTIONS_ENTRY_PARSER_NAME,
                    args -> new AbstractMap.SimpleEntry<>((long) args[0], Tuple.tuple((Integer)args[1], (ElasticsearchException)args[2])));

    static {
        STATUS_PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_CLUSTER);
        STATUS_PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX);
        STATUS_PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOWER_INDEX);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_ID);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_GLOBAL_CHECKPOINT_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_MAX_SEQ_NO_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAX_SEQ_NO_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_REQUESTED_SEQ_NO_FIELD);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), OUTSTANDING_READ_REQUESTS);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), OUTSTANDING_WRITE_REQUESTS);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), WRITE_BUFFER_OPERATION_COUNT_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), WRITE_BUFFER_SIZE_IN_BYTES_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAPPING_VERSION_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_SETTINGS_VERSION_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_ALIASES_VERSION_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_READ_TIME_MILLIS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), SUCCESSFUL_READ_REQUESTS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILED_READ_REQUESTS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_READ_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), BYTES_READ);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_WRITE_TIME_MILLIS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), SUCCESSFUL_WRITE_REQUESTS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILED_WRITE_REQUEST_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_WRITTEN);
        STATUS_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), READ_EXCEPTIONS_ENTRY_PARSER, READ_EXCEPTIONS);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_SINCE_LAST_READ_MILLIS_FIELD);
        STATUS_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
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

    public String getRemoteCluster() {
        return remoteCluster;
    }

    private final String leaderIndex;

    public String leaderIndex() {
        return leaderIndex;
    }

    private final String followerIndex;

    public String followerIndex() {
        return followerIndex;
    }

    private final int shardId;

    public int getShardId() {
        return shardId;
    }

    private final long leaderGlobalCheckpoint;

    public long leaderGlobalCheckpoint() {
        return leaderGlobalCheckpoint;
    }

    private final long leaderMaxSeqNo;

    public long leaderMaxSeqNo() {
        return leaderMaxSeqNo;
    }

    private final long followerGlobalCheckpoint;

    public long followerGlobalCheckpoint() {
        return followerGlobalCheckpoint;
    }

    private final long followerMaxSeqNo;

    public long followerMaxSeqNo() {
        return followerMaxSeqNo;
    }

    private final long lastRequestedSeqNo;

    public long lastRequestedSeqNo() {
        return lastRequestedSeqNo;
    }

    private final int outstandingReadRequests;

    public int outstandingReadRequests() {
        return outstandingReadRequests;
    }

    private final int outstandingWriteRequests;

    public int outstandingWriteRequests() {
        return outstandingWriteRequests;
    }

    private final int writeBufferOperationCount;

    public int writeBufferOperationCount() {
        return writeBufferOperationCount;
    }

    private final long writeBufferSizeInBytes;

    public long writeBufferSizeInBytes() {
        return writeBufferSizeInBytes;
    }

    private final long followerMappingVersion;

    public long followerMappingVersion() {
        return followerMappingVersion;
    }

    private final long followerSettingsVersion;

    public long followerSettingsVersion() {
        return followerSettingsVersion;
    }

    private final long followerAliasesVersion;

    public long followerAliasesVersion() {
        return followerAliasesVersion;
    }

    private final long totalReadTimeMillis;

    public long totalReadTimeMillis() {
        return totalReadTimeMillis;
    }

    private final long totalReadRemoteExecTimeMillis;

    public long totalReadRemoteExecTimeMillis() {
        return totalReadRemoteExecTimeMillis;
    }

    private final long successfulReadRequests;

    public long successfulReadRequests() {
        return successfulReadRequests;
    }

    private final long failedReadRequests;

    public long failedReadRequests() {
        return failedReadRequests;
    }

    private final long operationsReads;

    public long operationsReads() {
        return operationsReads;
    }

    private final long bytesRead;

    public long bytesRead() {
        return bytesRead;
    }

    private final long totalWriteTimeMillis;

    public long totalWriteTimeMillis() {
        return totalWriteTimeMillis;
    }

    private final long successfulWriteRequests;

    public long successfulWriteRequests() {
        return successfulWriteRequests;
    }

    private final long failedWriteRequests;

    public long failedWriteRequests() {
        return failedWriteRequests;
    }

    private final long operationWritten;

    public long operationWritten() {
        return operationWritten;
    }

    private final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions;

    public NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions() {
        return readExceptions;
    }

    private final long timeSinceLastReadMillis;

    public long timeSinceLastReadMillis() {
        return timeSinceLastReadMillis;
    }

    private final ElasticsearchException fatalException;

    public ElasticsearchException getFatalException() {
        return fatalException;
    }

    public ShardFollowNodeTaskStatus(
            final String remoteCluster,
            final String leaderIndex,
            final String followerIndex,
            final int shardId,
            final long leaderGlobalCheckpoint,
            final long leaderMaxSeqNo,
            final long followerGlobalCheckpoint,
            final long followerMaxSeqNo,
            final long lastRequestedSeqNo,
            final int outstandingReadRequests,
            final int outstandingWriteRequests,
            final int writeBufferOperationCount,
            final long writeBufferSizeInBytes,
            final long followerMappingVersion,
            final long followerSettingsVersion,
            final long followerAliasesVersion,
            final long totalReadTimeMillis,
            final long totalReadRemoteExecTimeMillis,
            final long successfulReadRequests,
            final long failedReadRequests,
            final long operationsReads,
            final long bytesRead,
            final long totalWriteTimeMillis,
            final long successfulWriteRequests,
            final long failedWriteRequests,
            final long operationWritten,
            final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions,
            final long timeSinceLastReadMillis,
            final ElasticsearchException fatalException) {
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
        this.readExceptions = Objects.requireNonNull(readExceptions);
        this.timeSinceLastReadMillis = timeSinceLastReadMillis;
        this.fatalException = fatalException;
    }

    public ShardFollowNodeTaskStatus(final StreamInput in) throws IOException {
        this.remoteCluster = in.readOptionalString();
        this.leaderIndex = in.readString();
        this.followerIndex = in.readString();
        this.shardId = in.readVInt();
        this.leaderGlobalCheckpoint = in.readZLong();
        this.leaderMaxSeqNo = in.readZLong();
        this.followerGlobalCheckpoint = in.readZLong();
        this.followerMaxSeqNo = in.readZLong();
        this.lastRequestedSeqNo = in.readZLong();
        this.outstandingReadRequests = in.readVInt();
        this.outstandingWriteRequests = in.readVInt();
        this.writeBufferOperationCount = in.readVInt();
        this.writeBufferSizeInBytes = in.readVLong();
        this.followerMappingVersion = in.readVLong();
        this.followerSettingsVersion = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            this.followerAliasesVersion = in.readVLong();
        } else {
            this.followerAliasesVersion = 0L;
        }
        this.totalReadTimeMillis = in.readVLong();
        this.totalReadRemoteExecTimeMillis = in.readVLong();
        this.successfulReadRequests = in.readVLong();
        this.failedReadRequests = in.readVLong();
        this.operationsReads = in.readVLong();
        this.bytesRead = in.readVLong();
        this.totalWriteTimeMillis = in.readVLong();
        this.successfulWriteRequests = in.readVLong();
        this.failedWriteRequests = in.readVLong();
        this.operationWritten = in.readVLong();
        this.readExceptions =
                new TreeMap<>(in.readMap(StreamInput::readVLong, stream -> Tuple.tuple(stream.readVInt(), stream.readException())));
        this.timeSinceLastReadMillis = in.readZLong();
        this.fatalException = in.readException();
    }

    @Override
    public String getWriteableName() {
        return STATUS_PARSER_NAME;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalString(remoteCluster);
        out.writeString(leaderIndex);
        out.writeString(followerIndex);
        out.writeVInt(shardId);
        out.writeZLong(leaderGlobalCheckpoint);
        out.writeZLong(leaderMaxSeqNo);
        out.writeZLong(followerGlobalCheckpoint);
        out.writeZLong(followerMaxSeqNo);
        out.writeZLong(lastRequestedSeqNo);
        out.writeVInt(outstandingReadRequests);
        out.writeVInt(outstandingWriteRequests);
        out.writeVInt(writeBufferOperationCount);
        out.writeVLong(writeBufferSizeInBytes);
        out.writeVLong(followerMappingVersion);
        out.writeVLong(followerSettingsVersion);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeVLong(followerAliasesVersion);
        }
        out.writeVLong(totalReadTimeMillis);
        out.writeVLong(totalReadRemoteExecTimeMillis);
        out.writeVLong(successfulReadRequests);
        out.writeVLong(failedReadRequests);
        out.writeVLong(operationsReads);
        out.writeVLong(bytesRead);
        out.writeVLong(totalWriteTimeMillis);
        out.writeVLong(successfulWriteRequests);
        out.writeVLong(failedWriteRequests);
        out.writeVLong(operationWritten);
        out.writeMap(
            readExceptions,
                StreamOutput::writeVLong,
                (stream, value) -> {
                    stream.writeVInt(value.v1());
                    stream.writeException(value.v2());
                });
        out.writeZLong(timeSinceLastReadMillis);
        out.writeException(fatalException);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            toXContentFragment(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(final XContentBuilder builder, final Params params) throws IOException {
        builder.field(LEADER_CLUSTER.getPreferredName(), remoteCluster);
        builder.field(LEADER_INDEX.getPreferredName(), leaderIndex);
        builder.field(FOLLOWER_INDEX.getPreferredName(), followerIndex);
        builder.field(SHARD_ID.getPreferredName(), shardId);
        builder.field(LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), leaderGlobalCheckpoint);
        builder.field(LEADER_MAX_SEQ_NO_FIELD.getPreferredName(), leaderMaxSeqNo);
        builder.field(FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), followerGlobalCheckpoint);
        builder.field(FOLLOWER_MAX_SEQ_NO_FIELD.getPreferredName(), followerMaxSeqNo);
        builder.field(LAST_REQUESTED_SEQ_NO_FIELD.getPreferredName(), lastRequestedSeqNo);
        builder.field(OUTSTANDING_READ_REQUESTS.getPreferredName(), outstandingReadRequests);
        builder.field(OUTSTANDING_WRITE_REQUESTS.getPreferredName(), outstandingWriteRequests);
        builder.field(WRITE_BUFFER_OPERATION_COUNT_FIELD.getPreferredName(), writeBufferOperationCount);
        builder.humanReadableField(
                WRITE_BUFFER_SIZE_IN_BYTES_FIELD.getPreferredName(),
                "write_buffer_size",
                new ByteSizeValue(writeBufferSizeInBytes));
        builder.field(FOLLOWER_MAPPING_VERSION_FIELD.getPreferredName(), followerMappingVersion);
        builder.field(FOLLOWER_SETTINGS_VERSION_FIELD.getPreferredName(), followerSettingsVersion);
        builder.field(FOLLOWER_ALIASES_VERSION_FIELD.getPreferredName(), followerAliasesVersion);
        builder.humanReadableField(
                TOTAL_READ_TIME_MILLIS_FIELD.getPreferredName(),
                "total_read_time",
                new TimeValue(totalReadTimeMillis, TimeUnit.MILLISECONDS));
        builder.humanReadableField(
                TOTAL_READ_REMOTE_EXEC_TIME_MILLIS_FIELD.getPreferredName(),
                "total_read_remote_exec_time",
                new TimeValue(totalReadRemoteExecTimeMillis, TimeUnit.MILLISECONDS));
        builder.field(SUCCESSFUL_READ_REQUESTS_FIELD.getPreferredName(), successfulReadRequests);
        builder.field(FAILED_READ_REQUESTS_FIELD.getPreferredName(), failedReadRequests);
        builder.field(OPERATIONS_READ_FIELD.getPreferredName(), operationsReads);
        builder.humanReadableField(
                BYTES_READ.getPreferredName(),
                "total_read",
                new ByteSizeValue(bytesRead, ByteSizeUnit.BYTES));
        builder.humanReadableField(
                TOTAL_WRITE_TIME_MILLIS_FIELD.getPreferredName(),
                "total_write_time",
                new TimeValue(totalWriteTimeMillis, TimeUnit.MILLISECONDS));
        builder.field(SUCCESSFUL_WRITE_REQUESTS_FIELD.getPreferredName(), successfulWriteRequests);
        builder.field(FAILED_WRITE_REQUEST_FIELD.getPreferredName(), failedWriteRequests);
        builder.field(OPERATIONS_WRITTEN.getPreferredName(), operationWritten);
        builder.startArray(READ_EXCEPTIONS.getPreferredName());
        {
            for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry : readExceptions.entrySet()) {
                builder.startObject();
                {
                    builder.field(READ_EXCEPTIONS_ENTRY_FROM_SEQ_NO.getPreferredName(), entry.getKey());
                    builder.field(READ_EXCEPTIONS_RETRIES.getPreferredName(), entry.getValue().v1());
                    builder.field(READ_EXCEPTIONS_ENTRY_EXCEPTION.getPreferredName());
                    builder.startObject();
                    {
                        ElasticsearchException.generateThrowableXContent(builder, params, entry.getValue().v2());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        builder.endArray();
        builder.humanReadableField(
                TIME_SINCE_LAST_READ_MILLIS_FIELD.getPreferredName(),
                "time_since_last_read",
                new TimeValue(timeSinceLastReadMillis, TimeUnit.MILLISECONDS));
        if (fatalException != null) {
            builder.field(FATAL_EXCEPTION.getPreferredName());
            builder.startObject();
            {
                ElasticsearchException.generateThrowableXContent(builder, params, fatalException);
            }
            builder.endObject();
        }
        return builder;
    }

    public static ShardFollowNodeTaskStatus fromXContent(final XContentParser parser) {
        return STATUS_PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ShardFollowNodeTaskStatus that = (ShardFollowNodeTaskStatus) o;
        String fatalExceptionMessage = fatalException != null ? fatalException.getMessage() : null;
        String otherFatalExceptionMessage = that.fatalException != null ? that.fatalException.getMessage() : null;
        return remoteCluster.equals(that.remoteCluster) &&
                leaderIndex.equals(that.leaderIndex) &&
                followerIndex.equals(that.followerIndex) &&
                shardId == that.shardId &&
                leaderGlobalCheckpoint == that.leaderGlobalCheckpoint &&
                leaderMaxSeqNo == that.leaderMaxSeqNo &&
                followerGlobalCheckpoint == that.followerGlobalCheckpoint &&
                followerMaxSeqNo == that.followerMaxSeqNo &&
                lastRequestedSeqNo == that.lastRequestedSeqNo &&
                outstandingReadRequests == that.outstandingReadRequests &&
                outstandingWriteRequests == that.outstandingWriteRequests &&
                writeBufferOperationCount == that.writeBufferOperationCount &&
                writeBufferSizeInBytes == that.writeBufferSizeInBytes &&
                followerMappingVersion == that.followerMappingVersion &&
                followerSettingsVersion == that.followerSettingsVersion &&
                followerAliasesVersion == that.followerAliasesVersion &&
                totalReadTimeMillis == that.totalReadTimeMillis &&
                totalReadRemoteExecTimeMillis == that.totalReadRemoteExecTimeMillis &&
                successfulReadRequests == that.successfulReadRequests &&
                failedReadRequests == that.failedReadRequests &&
                operationsReads == that.operationsReads &&
                bytesRead == that.bytesRead &&
                successfulWriteRequests == that.successfulWriteRequests &&
                failedWriteRequests == that.failedWriteRequests &&
                operationWritten == that.operationWritten &&
                /*
                 * ElasticsearchException does not implement equals so we will assume the fetch exceptions are equal if they are equal
                 * up to the key set and their messages.  Note that we are relying on the fact that the fetch exceptions are ordered by
                 * keys.
                 */
                readExceptions.keySet().equals(that.readExceptions.keySet()) &&
                getReadExceptionMessages(this).equals(getReadExceptionMessages(that)) &&
                timeSinceLastReadMillis == that.timeSinceLastReadMillis &&
                Objects.equals(fatalExceptionMessage, otherFatalExceptionMessage);
    }

    @Override
    public int hashCode() {
        String fatalExceptionMessage = fatalException != null ? fatalException.getMessage() : null;
        return Objects.hash(
                remoteCluster,
                leaderIndex,
                followerIndex,
                shardId,
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                outstandingReadRequests,
                outstandingWriteRequests,
                writeBufferOperationCount,
                writeBufferSizeInBytes,
                followerMappingVersion,
                followerSettingsVersion,
                followerAliasesVersion,
                totalReadTimeMillis,
                totalReadRemoteExecTimeMillis,
                successfulReadRequests,
                failedReadRequests,
                operationsReads,
                bytesRead,
                successfulWriteRequests,
                failedWriteRequests,
                operationWritten,
                /*
                 * ElasticsearchException does not implement hash code so we will compute the hash code based on the key set and the
                 * messages. Note that we are relying on the fact that the fetch exceptions are ordered by keys.
                 */
                readExceptions.keySet(),
                getReadExceptionMessages(this),
                timeSinceLastReadMillis,
                fatalExceptionMessage);
    }

    private static List<String> getReadExceptionMessages(final ShardFollowNodeTaskStatus status) {
        return status.readExceptions().values().stream().map(t -> t.v2().getMessage()).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
