/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ShardFollowTask implements XPackPlugin.XPackPersistentTaskParams {

    public static final String NAME = "xpack/ccr/shard_follow_task";

    // list of headers that will be stored when a job is created
    public static final Set<String> HEADER_FILTERS =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("es-security-runas-user", "_xpack_security_authentication")));

    static final ParseField LEADER_CLUSTER_ALIAS_FIELD = new ParseField("leader_cluster_alias");
    static final ParseField FOLLOW_SHARD_INDEX_FIELD = new ParseField("follow_shard_index");
    static final ParseField FOLLOW_SHARD_INDEX_UUID_FIELD = new ParseField("follow_shard_index_uuid");
    static final ParseField FOLLOW_SHARD_SHARDID_FIELD = new ParseField("follow_shard_shard");
    static final ParseField LEADER_SHARD_INDEX_FIELD = new ParseField("leader_shard_index");
    static final ParseField LEADER_SHARD_INDEX_UUID_FIELD = new ParseField("leader_shard_index_uuid");
    static final ParseField LEADER_SHARD_SHARDID_FIELD = new ParseField("leader_shard_shard");
    static final ParseField HEADERS = new ParseField("headers");
    public static final ParseField MAX_BATCH_OPERATION_COUNT = new ParseField("max_batch_operation_count");
    public static final ParseField MAX_CONCURRENT_READ_BATCHES = new ParseField("max_concurrent_read_batches");
    public static final ParseField MAX_BATCH_SIZE = new ParseField("max_batch_size");
    public static final ParseField MAX_CONCURRENT_WRITE_BATCHES = new ParseField("max_concurrent_write_batches");
    public static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
    public static final ParseField MAX_RETRY_DELAY = new ParseField("max_retry_delay");
    public static final ParseField POLL_TIMEOUT = new ParseField("poll_timeout");
    public static final ParseField RECORDED_HISTORY_UUID = new ParseField("recorded_history_uuid");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0], new ShardId((String) a[1], (String) a[2], (int) a[3]),
                    new ShardId((String) a[4], (String) a[5], (int) a[6]), (int) a[7], (int) a[8], (ByteSizeValue) a[9],
                (int) a[10], (int) a[11], (TimeValue) a[12], (TimeValue) a[13], (String) a[14], (Map<String, String>) a[15]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEADER_CLUSTER_ALIAS_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_BATCH_OPERATION_COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_CONCURRENT_READ_BATCHES);
        PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_BATCH_SIZE.getPreferredName()),
                MAX_BATCH_SIZE,
                ObjectParser.ValueType.STRING);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_CONCURRENT_WRITE_BATCHES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_WRITE_BUFFER_SIZE);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY.getPreferredName()),
            MAX_RETRY_DELAY, ObjectParser.ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), POLL_TIMEOUT.getPreferredName()),
                POLL_TIMEOUT, ObjectParser.ValueType.STRING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), RECORDED_HISTORY_UUID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String leaderClusterAlias;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final int maxBatchOperationCount;
    private final int maxConcurrentReadBatches;
    private final ByteSizeValue maxBatchSize;
    private final int maxConcurrentWriteBatches;
    private final int maxWriteBufferSize;
    private final TimeValue maxRetryDelay;
    private final TimeValue pollTimeout;
    private final String recordedLeaderIndexHistoryUUID;
    private final Map<String, String> headers;

    ShardFollowTask(
            final String leaderClusterAlias,
            final ShardId followShardId,
            final ShardId leaderShardId,
            final int maxBatchOperationCount,
            final int maxConcurrentReadBatches,
            final ByteSizeValue maxBatchSize,
            final int maxConcurrentWriteBatches,
            final int maxWriteBufferSize,
            final TimeValue maxRetryDelay,
            final TimeValue pollTimeout,
            final String recordedLeaderIndexHistoryUUID,
            final Map<String, String> headers) {
        this.leaderClusterAlias = leaderClusterAlias;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxBatchOperationCount = maxBatchOperationCount;
        this.maxConcurrentReadBatches = maxConcurrentReadBatches;
        this.maxBatchSize = maxBatchSize;
        this.maxConcurrentWriteBatches = maxConcurrentWriteBatches;
        this.maxWriteBufferSize = maxWriteBufferSize;
        this.maxRetryDelay = maxRetryDelay;
        this.pollTimeout = pollTimeout;
        this.recordedLeaderIndexHistoryUUID = recordedLeaderIndexHistoryUUID;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.leaderClusterAlias = in.readOptionalString();
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxBatchOperationCount = in.readVInt();
        this.maxConcurrentReadBatches = in.readVInt();
        this.maxBatchSize = new ByteSizeValue(in);
        this.maxConcurrentWriteBatches = in.readVInt();
        this.maxWriteBufferSize = in.readVInt();
        this.maxRetryDelay = in.readTimeValue();
        this.pollTimeout = in.readTimeValue();
        this.recordedLeaderIndexHistoryUUID = in.readString();
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    public String getLeaderClusterAlias() {
        return leaderClusterAlias;
    }

    public ShardId getFollowShardId() {
        return followShardId;
    }

    public ShardId getLeaderShardId() {
        return leaderShardId;
    }

    public int getMaxBatchOperationCount() {
        return maxBatchOperationCount;
    }

    public int getMaxConcurrentReadBatches() {
        return maxConcurrentReadBatches;
    }

    public int getMaxConcurrentWriteBatches() {
        return maxConcurrentWriteBatches;
    }

    public int getMaxWriteBufferSize() {
        return maxWriteBufferSize;
    }

    public ByteSizeValue getMaxBatchSize() {
        return maxBatchSize;
    }

    public TimeValue getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public TimeValue getPollTimeout() {
        return pollTimeout;
    }

    public String getTaskId() {
        return followShardId.getIndex().getUUID() + "-" + followShardId.getId();
    }

    public String getRecordedLeaderIndexHistoryUUID() {
        return recordedLeaderIndexHistoryUUID;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(leaderClusterAlias);
        followShardId.writeTo(out);
        leaderShardId.writeTo(out);
        out.writeVLong(maxBatchOperationCount);
        out.writeVInt(maxConcurrentReadBatches);
        maxBatchSize.writeTo(out);
        out.writeVInt(maxConcurrentWriteBatches);
        out.writeVInt(maxWriteBufferSize);
        out.writeTimeValue(maxRetryDelay);
        out.writeTimeValue(pollTimeout);
        out.writeString(recordedLeaderIndexHistoryUUID);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ShardFollowTask fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (leaderClusterAlias != null) {
            builder.field(LEADER_CLUSTER_ALIAS_FIELD.getPreferredName(), leaderClusterAlias);
        }
        builder.field(FOLLOW_SHARD_INDEX_FIELD.getPreferredName(), followShardId.getIndex().getName());
        builder.field(FOLLOW_SHARD_INDEX_UUID_FIELD.getPreferredName(), followShardId.getIndex().getUUID());
        builder.field(FOLLOW_SHARD_SHARDID_FIELD.getPreferredName(), followShardId.id());
        builder.field(LEADER_SHARD_INDEX_FIELD.getPreferredName(), leaderShardId.getIndex().getName());
        builder.field(LEADER_SHARD_INDEX_UUID_FIELD.getPreferredName(), leaderShardId.getIndex().getUUID());
        builder.field(LEADER_SHARD_SHARDID_FIELD.getPreferredName(), leaderShardId.id());
        builder.field(MAX_BATCH_OPERATION_COUNT.getPreferredName(), maxBatchOperationCount);
        builder.field(MAX_CONCURRENT_READ_BATCHES.getPreferredName(), maxConcurrentReadBatches);
        builder.field(MAX_BATCH_SIZE.getPreferredName(), maxBatchSize.getStringRep());
        builder.field(MAX_CONCURRENT_WRITE_BATCHES.getPreferredName(), maxConcurrentWriteBatches);
        builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize);
        builder.field(MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
        builder.field(POLL_TIMEOUT.getPreferredName(), pollTimeout.getStringRep());
        builder.field(RECORDED_HISTORY_UUID.getPreferredName(), recordedLeaderIndexHistoryUUID);
        builder.field(HEADERS.getPreferredName(), headers);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardFollowTask that = (ShardFollowTask) o;
        return Objects.equals(leaderClusterAlias, that.leaderClusterAlias) &&
                Objects.equals(followShardId, that.followShardId) &&
                Objects.equals(leaderShardId, that.leaderShardId) &&
                maxBatchOperationCount == that.maxBatchOperationCount &&
                maxConcurrentReadBatches == that.maxConcurrentReadBatches &&
                maxConcurrentWriteBatches == that.maxConcurrentWriteBatches &&
                maxBatchSize.equals(that.maxBatchSize) &&
                maxWriteBufferSize == that.maxWriteBufferSize &&
                Objects.equals(maxRetryDelay, that.maxRetryDelay) &&
                Objects.equals(pollTimeout, that.pollTimeout) &&
                Objects.equals(recordedLeaderIndexHistoryUUID, that.recordedLeaderIndexHistoryUUID) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                leaderClusterAlias,
                followShardId,
                leaderShardId,
                maxBatchOperationCount,
                maxConcurrentReadBatches,
                maxConcurrentWriteBatches,
                maxBatchSize,
                maxWriteBufferSize,
                maxRetryDelay,
                pollTimeout,
                recordedLeaderIndexHistoryUUID,
                headers);
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_5_0;
    }
}
