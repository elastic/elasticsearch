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

    static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
    static final ParseField FOLLOW_SHARD_INDEX_FIELD = new ParseField("follow_shard_index");
    static final ParseField FOLLOW_SHARD_INDEX_UUID_FIELD = new ParseField("follow_shard_index_uuid");
    static final ParseField FOLLOW_SHARD_SHARDID_FIELD = new ParseField("follow_shard_shard");
    static final ParseField LEADER_SHARD_INDEX_FIELD = new ParseField("leader_shard_index");
    static final ParseField LEADER_SHARD_INDEX_UUID_FIELD = new ParseField("leader_shard_index_uuid");
    static final ParseField LEADER_SHARD_SHARDID_FIELD = new ParseField("leader_shard_shard");
    static final ParseField HEADERS = new ParseField("headers");
    public static final ParseField MAX_READ_REQUEST_OPERATION_COUNT = new ParseField("max_read_request_operation_count");
    public static final ParseField MAX_READ_REQUEST_SIZE = new ParseField("max_read_request_size");
    public static final ParseField MAX_OUTSTANDING_READ_REQUESTS = new ParseField("max_outstanding_read_requests");
    public static final ParseField MAX_WRITE_REQUEST_OPERATION_COUNT = new ParseField("max_write_request_operation_count");
    public static final ParseField MAX_WRITE_REQUEST_SIZE = new ParseField("max_write_request_size");
    public static final ParseField MAX_OUTSTANDING_WRITE_REQUESTS = new ParseField("max_outstanding_write_requests");
    public static final ParseField MAX_WRITE_BUFFER_COUNT = new ParseField("max_write_buffer_count");
    public static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
    public static final ParseField MAX_RETRY_DELAY = new ParseField("max_retry_delay");
    public static final ParseField READ_POLL_TIMEOUT = new ParseField("read_poll_timeout");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0],
                new ShardId((String) a[1], (String) a[2], (int) a[3]), new ShardId((String) a[4], (String) a[5], (int) a[6]),
                (int) a[7], (ByteSizeValue) a[8], (int) a[9], (int) a[10], (ByteSizeValue) a[11], (int) a[12],
                (int) a[13], (ByteSizeValue) a[14], (TimeValue) a[15], (TimeValue) a[16], (Map<String, String>) a[17]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REMOTE_CLUSTER_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_READ_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_READ_REQUEST_SIZE.getPreferredName()),
                MAX_READ_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_OUTSTANDING_READ_REQUESTS);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_WRITE_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_REQUEST_SIZE.getPreferredName()),
                MAX_WRITE_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_OUTSTANDING_WRITE_REQUESTS);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_WRITE_BUFFER_COUNT);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY.getPreferredName()),
            MAX_RETRY_DELAY, ObjectParser.ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), READ_POLL_TIMEOUT.getPreferredName()),
            READ_POLL_TIMEOUT, ObjectParser.ValueType.STRING);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String remoteCluster;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final int maxReadRequestOperationCount;
    private final ByteSizeValue maxReadRequestSize;
    private final int maxOutstandingReadRequests;
    private final int maxWriteRequestOperationCount;
    private final ByteSizeValue maxWriteRequestSize;
    private final int maxOutstandingWriteRequests;
    private final int maxWriteBufferCount;
    private final ByteSizeValue maxWriteBufferSize;
    private final TimeValue maxRetryDelay;
    private final TimeValue readPollTimeout;
    private final Map<String, String> headers;

    ShardFollowTask(
            final String remoteCluster,
            final ShardId followShardId,
            final ShardId leaderShardId,
            final int maxReadRequestOperationCount,
            final ByteSizeValue maxReadRequestSize,
            final int maxOutstandingReadRequests,
            final int maxWriteRequestOperationCount,
            final ByteSizeValue maxWriteRequestSize,
            final int maxOutstandingWriteRequests,
            final int maxWriteBufferCount,
            final ByteSizeValue maxWriteBufferSize,
            final TimeValue maxRetryDelay,
            final TimeValue readPollTimeout,
            final Map<String, String> headers) {
        this.remoteCluster = remoteCluster;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxReadRequestOperationCount = maxReadRequestOperationCount;
        this.maxReadRequestSize = maxReadRequestSize;
        this.maxOutstandingReadRequests = maxOutstandingReadRequests;
        this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
        this.maxWriteRequestSize = maxWriteRequestSize;
        this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
        this.maxWriteBufferCount = maxWriteBufferCount;
        this.maxWriteBufferSize = maxWriteBufferSize;
        this.maxRetryDelay = maxRetryDelay;
        this.readPollTimeout = readPollTimeout;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.remoteCluster = in.readString();
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxReadRequestOperationCount = in.readVInt();
        this.maxReadRequestSize = new ByteSizeValue(in);
        this.maxOutstandingReadRequests = in.readVInt();
        this.maxWriteRequestOperationCount = in.readVInt();
        this.maxWriteRequestSize = new ByteSizeValue(in);
        this.maxOutstandingWriteRequests = in.readVInt();
        this.maxWriteBufferCount = in.readVInt();
        this.maxWriteBufferSize = new ByteSizeValue(in);
        this.maxRetryDelay = in.readTimeValue();
        this.readPollTimeout = in.readTimeValue();
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    public ShardId getFollowShardId() {
        return followShardId;
    }

    public ShardId getLeaderShardId() {
        return leaderShardId;
    }

    public int getMaxReadRequestOperationCount() {
        return maxReadRequestOperationCount;
    }

    public int getMaxOutstandingReadRequests() {
        return maxOutstandingReadRequests;
    }

    public int getMaxWriteRequestOperationCount() {
        return maxWriteRequestOperationCount;
    }

    public ByteSizeValue getMaxWriteRequestSize() {
        return maxWriteRequestSize;
    }

    public int getMaxOutstandingWriteRequests() {
        return maxOutstandingWriteRequests;
    }

    public int getMaxWriteBufferCount() {
        return maxWriteBufferCount;
    }

    public ByteSizeValue getMaxWriteBufferSize() {
        return maxWriteBufferSize;
    }

    public ByteSizeValue getMaxReadRequestSize() {
        return maxReadRequestSize;
    }

    public TimeValue getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public TimeValue getReadPollTimeout() {
        return readPollTimeout;
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
        out.writeString(remoteCluster);
        followShardId.writeTo(out);
        leaderShardId.writeTo(out);
        out.writeVLong(maxReadRequestOperationCount);
        maxReadRequestSize.writeTo(out);
        out.writeVInt(maxOutstandingReadRequests);
        out.writeVLong(maxWriteRequestOperationCount);
        maxWriteRequestSize.writeTo(out);
        out.writeVInt(maxOutstandingWriteRequests);
        out.writeVInt(maxWriteBufferCount);
        maxWriteBufferSize.writeTo(out);
        out.writeTimeValue(maxRetryDelay);
        out.writeTimeValue(readPollTimeout);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ShardFollowTask fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
        builder.field(FOLLOW_SHARD_INDEX_FIELD.getPreferredName(), followShardId.getIndex().getName());
        builder.field(FOLLOW_SHARD_INDEX_UUID_FIELD.getPreferredName(), followShardId.getIndex().getUUID());
        builder.field(FOLLOW_SHARD_SHARDID_FIELD.getPreferredName(), followShardId.id());
        builder.field(LEADER_SHARD_INDEX_FIELD.getPreferredName(), leaderShardId.getIndex().getName());
        builder.field(LEADER_SHARD_INDEX_UUID_FIELD.getPreferredName(), leaderShardId.getIndex().getUUID());
        builder.field(LEADER_SHARD_SHARDID_FIELD.getPreferredName(), leaderShardId.id());
        builder.field(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
        builder.field(MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
        builder.field(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxOutstandingReadRequests);
        builder.field(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
        builder.field(MAX_WRITE_REQUEST_SIZE.getPreferredName(), maxWriteRequestSize.getStringRep());
        builder.field(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxOutstandingWriteRequests);
        builder.field(MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
        builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
        builder.field(MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
        builder.field(READ_POLL_TIMEOUT.getPreferredName(), readPollTimeout.getStringRep());
        builder.field(HEADERS.getPreferredName(), headers);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardFollowTask that = (ShardFollowTask) o;
        return Objects.equals(remoteCluster, that.remoteCluster) &&
                Objects.equals(followShardId, that.followShardId) &&
                Objects.equals(leaderShardId, that.leaderShardId) &&
                maxReadRequestOperationCount == that.maxReadRequestOperationCount &&
                maxReadRequestSize.equals(that.maxReadRequestSize) &&
                maxOutstandingReadRequests == that.maxOutstandingReadRequests &&
                maxWriteRequestOperationCount == that.maxWriteRequestOperationCount &&
                maxWriteRequestSize.equals(that.maxWriteRequestSize) &&
                maxOutstandingWriteRequests == that.maxOutstandingWriteRequests &&
                maxWriteBufferCount == that.maxWriteBufferCount &&
                maxWriteBufferSize.equals(that.maxWriteBufferSize) &&
                Objects.equals(maxRetryDelay, that.maxRetryDelay) &&
                Objects.equals(readPollTimeout, that.readPollTimeout) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                remoteCluster,
                followShardId,
                leaderShardId,
                maxReadRequestOperationCount,
                maxReadRequestSize,
                maxOutstandingReadRequests,
                maxWriteRequestOperationCount,
                maxWriteRequestSize,
                maxOutstandingWriteRequests,
                maxWriteBufferCount,
                maxWriteBufferSize,
                maxRetryDelay,
                readPollTimeout,
                headers
        );
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_5_0;
    }
}
