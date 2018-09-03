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
    public static final ParseField MAX_BATCH_SIZE_IN_BYTES = new ParseField("max_batch_size_in_bytes");
    public static final ParseField MAX_CONCURRENT_WRITE_BATCHES = new ParseField("max_concurrent_write_batches");
    public static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
    public static final ParseField RETRY_TIMEOUT = new ParseField("retry_timeout");
    public static final ParseField IDLE_SHARD_RETRY_DELAY = new ParseField("idle_shard_retry_delay");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0], new ShardId((String) a[1], (String) a[2], (int) a[3]),
                    new ShardId((String) a[4], (String) a[5], (int) a[6]), (int) a[7], (int) a[8], (long) a[9],
                (int) a[10], (int) a[11], (TimeValue) a[12], (TimeValue) a[13], (Map<String, String>) a[14]));

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
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BATCH_SIZE_IN_BYTES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_CONCURRENT_WRITE_BATCHES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_WRITE_BUFFER_SIZE);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), RETRY_TIMEOUT.getPreferredName()),
            RETRY_TIMEOUT, ObjectParser.ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), IDLE_SHARD_RETRY_DELAY.getPreferredName()),
            IDLE_SHARD_RETRY_DELAY, ObjectParser.ValueType.STRING);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String leaderClusterAlias;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final int maxBatchOperationCount;
    private final int maxConcurrentReadBatches;
    private final long maxBatchSizeInBytes;
    private final int maxConcurrentWriteBatches;
    private final int maxWriteBufferSize;
    private final TimeValue retryTimeout;
    private final TimeValue idleShardRetryDelay;
    private final Map<String, String> headers;

    ShardFollowTask(String leaderClusterAlias, ShardId followShardId, ShardId leaderShardId, int maxBatchOperationCount,
                    int maxConcurrentReadBatches, long maxBatchSizeInBytes, int maxConcurrentWriteBatches,
                    int maxWriteBufferSize, TimeValue retryTimeout, TimeValue idleShardRetryDelay, Map<String, String> headers) {
        this.leaderClusterAlias = leaderClusterAlias;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxBatchOperationCount = maxBatchOperationCount;
        this.maxConcurrentReadBatches = maxConcurrentReadBatches;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxConcurrentWriteBatches = maxConcurrentWriteBatches;
        this.maxWriteBufferSize = maxWriteBufferSize;
        this.retryTimeout = retryTimeout;
        this.idleShardRetryDelay = idleShardRetryDelay;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.leaderClusterAlias = in.readOptionalString();
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxBatchOperationCount = in.readVInt();
        this.maxConcurrentReadBatches = in.readVInt();
        this.maxBatchSizeInBytes = in.readVLong();
        this.maxConcurrentWriteBatches = in.readVInt();
        this.maxWriteBufferSize = in.readVInt();
        this.retryTimeout = in.readTimeValue();
        this.idleShardRetryDelay = in.readTimeValue();
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

    public long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public TimeValue getRetryTimeout() {
        return retryTimeout;
    }

    public TimeValue getIdleShardRetryDelay() {
        return idleShardRetryDelay;
    }

    public String getTaskId() {
        return followShardId.getIndex().getUUID() + "-" + followShardId.getId();
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
        out.writeVLong(maxBatchSizeInBytes);
        out.writeVInt(maxConcurrentWriteBatches);
        out.writeVInt(maxWriteBufferSize);
        out.writeTimeValue(retryTimeout);
        out.writeTimeValue(idleShardRetryDelay);
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
        builder.field(MAX_BATCH_SIZE_IN_BYTES.getPreferredName(), maxBatchSizeInBytes);
        builder.field(MAX_CONCURRENT_WRITE_BATCHES.getPreferredName(), maxConcurrentWriteBatches);
        builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize);
        builder.field(RETRY_TIMEOUT.getPreferredName(), retryTimeout.getStringRep());
        builder.field(IDLE_SHARD_RETRY_DELAY.getPreferredName(), idleShardRetryDelay.getStringRep());
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
                maxBatchSizeInBytes == that.maxBatchSizeInBytes &&
                maxWriteBufferSize == that.maxWriteBufferSize &&
                Objects.equals(retryTimeout, that.retryTimeout) &&
                Objects.equals(idleShardRetryDelay, that.idleShardRetryDelay) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderClusterAlias, followShardId, leaderShardId, maxBatchOperationCount, maxConcurrentReadBatches,
            maxConcurrentWriteBatches, maxBatchSizeInBytes, maxWriteBufferSize, retryTimeout, idleShardRetryDelay, headers);
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_4_0;
    }
}
