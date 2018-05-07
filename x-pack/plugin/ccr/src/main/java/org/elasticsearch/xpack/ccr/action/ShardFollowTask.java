/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ShardFollowTask implements PersistentTaskParams {

    public static final String NAME = "shard_follow";

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
    public static final ParseField MAX_CHUNK_SIZE = new ParseField("max_chunk_size");
    public static final ParseField NUM_CONCURRENT_CHUNKS = new ParseField("max_concurrent_chunks");
    public static final ParseField PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST = new ParseField("processor_max_translog_bytes");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0], new ShardId((String) a[1], (String) a[2], (int) a[3]),
                    new ShardId((String) a[4], (String) a[5], (int) a[6]), (long) a[7], (int) a[8], (long) a[9],
                (Map<String, String>) a[10]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEADER_CLUSTER_ALIAS_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_CHUNK_SIZE);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CONCURRENT_CHUNKS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String leaderClusterAlias;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final long maxChunkSize;
    private final int numConcurrentChunks;
    private final long processorMaxTranslogBytes;
    private final Map<String, String> headers;

    ShardFollowTask(String leaderClusterAlias, ShardId followShardId, ShardId leaderShardId, long maxChunkSize,
                    int numConcurrentChunks, long processorMaxTranslogBytes, Map<String, String> headers) {
        this.leaderClusterAlias = leaderClusterAlias;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxChunkSize = maxChunkSize;
        this.numConcurrentChunks = numConcurrentChunks;
        this.processorMaxTranslogBytes = processorMaxTranslogBytes;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.leaderClusterAlias = in.readOptionalString();
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxChunkSize = in.readVLong();
        this.numConcurrentChunks = in.readVInt();
        this.processorMaxTranslogBytes = in.readVLong();
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

    public long getMaxChunkSize() {
        return maxChunkSize;
    }

    public int getNumConcurrentChunks() {
        return numConcurrentChunks;
    }

    public long getProcessorMaxTranslogBytes() {
        return processorMaxTranslogBytes;
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
        out.writeVLong(maxChunkSize);
        out.writeVInt(numConcurrentChunks);
        out.writeVLong(processorMaxTranslogBytes);
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
        builder.field(MAX_CHUNK_SIZE.getPreferredName(), maxChunkSize);
        builder.field(NUM_CONCURRENT_CHUNKS.getPreferredName(), numConcurrentChunks);
        builder.field(PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST.getPreferredName(), processorMaxTranslogBytes);
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
                maxChunkSize == that.maxChunkSize &&
                numConcurrentChunks == that.numConcurrentChunks &&
                processorMaxTranslogBytes == that.processorMaxTranslogBytes &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderClusterAlias, followShardId, leaderShardId, maxChunkSize, numConcurrentChunks,
                processorMaxTranslogBytes, headers);
    }

    public String toString() {
        return Strings.toString(this);
    }

}
