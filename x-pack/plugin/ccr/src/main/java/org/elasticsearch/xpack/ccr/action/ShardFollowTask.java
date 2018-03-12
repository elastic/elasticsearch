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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.Objects;

public class ShardFollowTask implements PersistentTaskParams {

    public static final String NAME = "shard_follow";

    static final ParseField FOLLOW_SHARD_INDEX_FIELD = new ParseField("follow_shard_index");
    static final ParseField FOLLOW_SHARD_INDEX_UUID_FIELD = new ParseField("follow_shard_index_uuid");
    static final ParseField FOLLOW_SHARD_SHARDID_FIELD = new ParseField("follow_shard_shard");
    static final ParseField LEADER_SHARD_INDEX_FIELD = new ParseField("leader_shard_index");
    static final ParseField LEADER_SHARD_INDEX_UUID_FIELD = new ParseField("leader_shard_index_uuid");
    static final ParseField LEADER_SHARD_SHARDID_FIELD = new ParseField("leader_shard_shard");
    public static final ParseField MAX_CHUNK_SIZE = new ParseField("max_chunk_size");
    public static final ParseField NUM_CONCURRENT_CHUNKS = new ParseField("max_concurrent_chunks");
    public static final ParseField PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST = new ParseField("processor_max_translog_bytes");

    public static ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask(new ShardId((String) a[0], (String) a[1], (int) a[2]),
                    new ShardId((String) a[3], (String) a[4], (int) a[5]), (long) a[6], (int) a[7], (long) a[8]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_CHUNK_SIZE);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CONCURRENT_CHUNKS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST);
    }

    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final long maxChunkSize;
    private final int numConcurrentChunks;
    private final long processorMaxTranslogBytes;

    ShardFollowTask(ShardId followShardId, ShardId leaderShardId, long maxChunkSize, int numConcurrentChunks,
                    long processorMaxTranslogBytes) {
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.maxChunkSize = maxChunkSize;
        this.numConcurrentChunks = numConcurrentChunks;
        this.processorMaxTranslogBytes = processorMaxTranslogBytes;
    }

    public ShardFollowTask(StreamInput in) throws IOException {
        this.followShardId = ShardId.readShardId(in);
        this.leaderShardId = ShardId.readShardId(in);
        this.maxChunkSize = in.readVLong();
        this.numConcurrentChunks = in.readVInt();
        this.processorMaxTranslogBytes = in.readVLong();
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        followShardId.writeTo(out);
        leaderShardId.writeTo(out);
        out.writeVLong(maxChunkSize);
        out.writeVInt(numConcurrentChunks);
        out.writeVLong(processorMaxTranslogBytes);
    }

    public static ShardFollowTask fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FOLLOW_SHARD_INDEX_FIELD.getPreferredName(), followShardId.getIndex().getName());
        builder.field(FOLLOW_SHARD_INDEX_UUID_FIELD.getPreferredName(), followShardId.getIndex().getUUID());
        builder.field(FOLLOW_SHARD_SHARDID_FIELD.getPreferredName(), followShardId.id());
        builder.field(LEADER_SHARD_INDEX_FIELD.getPreferredName(), leaderShardId.getIndex().getName());
        builder.field(LEADER_SHARD_INDEX_UUID_FIELD.getPreferredName(), leaderShardId.getIndex().getUUID());
        builder.field(LEADER_SHARD_SHARDID_FIELD.getPreferredName(), leaderShardId.id());
        builder.field(MAX_CHUNK_SIZE.getPreferredName(), maxChunkSize);
        builder.field(NUM_CONCURRENT_CHUNKS.getPreferredName(), numConcurrentChunks);
        builder.field(PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST.getPreferredName(), processorMaxTranslogBytes);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardFollowTask that = (ShardFollowTask) o;
        return Objects.equals(followShardId, that.followShardId) &&
                Objects.equals(leaderShardId, that.leaderShardId) &&
                maxChunkSize == that.maxChunkSize &&
                numConcurrentChunks == that.numConcurrentChunks &&
                processorMaxTranslogBytes == that.processorMaxTranslogBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(followShardId, leaderShardId, maxChunkSize, numConcurrentChunks, processorMaxTranslogBytes);
    }

    public String toString() {
        return Strings.toString(this);
    }

    public static class Status implements Task.Status {

        static final ParseField PROCESSED_GLOBAL_CHECKPOINT_FIELD = new ParseField("processed_global_checkpoint");

        static final ObjectParser<Status, Void> PARSER = new ObjectParser<>(NAME, Status::new);

        static {
            PARSER.declareLong(Status::setProcessedGlobalCheckpoint, PROCESSED_GLOBAL_CHECKPOINT_FIELD);
        }

        private long processedGlobalCheckpoint;

        public Status() {
        }

        public Status(StreamInput in) throws IOException {
            processedGlobalCheckpoint = in.readZLong();
        }

        public long getProcessedGlobalCheckpoint() {
            return processedGlobalCheckpoint;
        }

        public void setProcessedGlobalCheckpoint(long processedGlobalCheckpoint) {
            this.processedGlobalCheckpoint = processedGlobalCheckpoint;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(processedGlobalCheckpoint);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PROCESSED_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), processedGlobalCheckpoint);
            return builder.endObject();
        }

        public static Status fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return processedGlobalCheckpoint == status.processedGlobalCheckpoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processedGlobalCheckpoint);
        }

        public String toString() {
            return Strings.toString(this);
        }
    }
}
