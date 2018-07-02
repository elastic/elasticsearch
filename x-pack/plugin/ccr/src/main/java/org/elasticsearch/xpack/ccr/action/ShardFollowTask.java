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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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
    public static final ParseField MAX_OPERATION_COUNT = new ParseField("max_operation_count");
    public static final ParseField MAX_CONCURRENT_READS = new ParseField("max_concurrent_reads");
    public static final ParseField MAX_OPERATION_SIZE_IN_BYTES = new ParseField("max_operation_size_in_bytes");
    public static final ParseField MAX_WRITE_SIZE = new ParseField("max_write_size");
    public static final ParseField MAX_CONCURRENT_WRITES = new ParseField("max_concurrent_writes");
    public static final ParseField MAX_BUFFER_SIZE = new ParseField("max_buffer_size");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0], new ShardId((String) a[1], (String) a[2], (int) a[3]),
                    new ShardId((String) a[4], (String) a[5], (int) a[6]), (int) a[7], (int) a[8], (long) a[9],
                (int) a[10], (int) a[11], (int) a[12], (Map<String, String>) a[13]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEADER_CLUSTER_ALIAS_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_OPERATION_COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_CONCURRENT_READS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_OPERATION_SIZE_IN_BYTES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_WRITE_SIZE);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_CONCURRENT_WRITES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_BUFFER_SIZE);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String leaderClusterAlias;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final int maxReadSize;
    private final int maxConcurrentReads;
    private final long maxOperationSizeInBytes;
    private final int maxWriteSize;
    private final int maxConcurrentWrites;
    private final int maxBufferSize;
    private final Map<String, String> headers;

    ShardFollowTask(String leaderClusterAlias, ShardId followShardId, ShardId leaderShardId, int maxReadSize,
                    int maxConcurrentReads, long maxOperationSizeInBytes, int maxWriteSize, int maxConcurrentWrites,
                    int maxBufferSize, Map<String, String> headers) {
        this.leaderClusterAlias = leaderClusterAlias;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxReadSize = maxReadSize;
        this.maxConcurrentReads = maxConcurrentReads;
        this.maxOperationSizeInBytes = maxOperationSizeInBytes;
        this.maxWriteSize = maxWriteSize;
        this.maxConcurrentWrites = maxConcurrentWrites;
        this.maxBufferSize = maxBufferSize;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.leaderClusterAlias = in.readOptionalString();
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxReadSize = in.readVInt();
        this.maxConcurrentReads = in.readVInt();
        this.maxOperationSizeInBytes = in.readVLong();
        this.maxWriteSize = in.readVInt();
        this.maxConcurrentWrites= in.readVInt();
        this.maxBufferSize = in.readVInt();
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

    public int getMaxReadSize() {
        return maxReadSize;
    }

    public int getMaxWriteSize() {
        return maxWriteSize;
    }

    public int getMaxConcurrentReads() {
        return maxConcurrentReads;
    }

    public int getMaxConcurrentWrites() {
        return maxConcurrentWrites;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getMaxOperationSizeInBytes() {
        return maxOperationSizeInBytes;
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
        out.writeVLong(maxReadSize);
        out.writeVInt(maxConcurrentReads);
        out.writeVLong(maxOperationSizeInBytes);
        out.writeVInt(maxWriteSize);
        out.writeVInt(maxConcurrentWrites);
        out.writeVInt(maxBufferSize);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ShardFollowTask fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (leaderClusterAlias != null) {
                builder.field(LEADER_CLUSTER_ALIAS_FIELD.getPreferredName(), leaderClusterAlias);
            }
        }
        {
            builder.field(FOLLOW_SHARD_INDEX_FIELD.getPreferredName(), followShardId.getIndex().getName());
        }
        {
            builder.field(FOLLOW_SHARD_INDEX_UUID_FIELD.getPreferredName(), followShardId.getIndex().getUUID());
        }
        {
            builder.field(FOLLOW_SHARD_SHARDID_FIELD.getPreferredName(), followShardId.id());
        }
        {
            builder.field(LEADER_SHARD_INDEX_FIELD.getPreferredName(), leaderShardId.getIndex().getName());
        }
        {
            builder.field(LEADER_SHARD_INDEX_UUID_FIELD.getPreferredName(), leaderShardId.getIndex().getUUID());
        }
        {
            builder.field(LEADER_SHARD_SHARDID_FIELD.getPreferredName(), leaderShardId.id());
        }
        {
            builder.field(MAX_OPERATION_COUNT.getPreferredName(), maxReadSize);
        }
        {
            builder.field(MAX_CONCURRENT_READS.getPreferredName(), maxConcurrentReads);
        }
        {
            builder.field(MAX_OPERATION_SIZE_IN_BYTES.getPreferredName(), maxOperationSizeInBytes);
        }
        {
            builder.field(MAX_WRITE_SIZE.getPreferredName(), maxWriteSize);
        }
        {
            builder.field(MAX_CONCURRENT_WRITES.getPreferredName(), maxConcurrentWrites);
        }
        {
            builder.field(MAX_BUFFER_SIZE.getPreferredName(), maxBufferSize);
        }
        {
            builder.field(HEADERS.getPreferredName(), headers);
        }
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
                maxReadSize == that.maxReadSize &&
                maxConcurrentReads == that.maxConcurrentReads &&
                maxWriteSize == that.maxWriteSize &&
                maxConcurrentWrites == that.maxConcurrentWrites &&
                maxOperationSizeInBytes == that.maxOperationSizeInBytes &&
                maxBufferSize == that.maxBufferSize &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderClusterAlias, followShardId, leaderShardId, maxReadSize, maxConcurrentReads,
            maxWriteSize, maxConcurrentWrites, maxOperationSizeInBytes, maxBufferSize, headers);
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_4_0;
    }
}
