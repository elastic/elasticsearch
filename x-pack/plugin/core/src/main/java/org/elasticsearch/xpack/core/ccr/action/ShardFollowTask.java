/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class ShardFollowTask extends ImmutableFollowParameters implements PersistentTaskParams {

    public static final String NAME = "xpack/ccr/shard_follow_task";

    private static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
    private static final ParseField FOLLOW_SHARD_INDEX_FIELD = new ParseField("follow_shard_index");
    private static final ParseField FOLLOW_SHARD_INDEX_UUID_FIELD = new ParseField("follow_shard_index_uuid");
    private static final ParseField FOLLOW_SHARD_SHARDID_FIELD = new ParseField("follow_shard_shard");
    private static final ParseField LEADER_SHARD_INDEX_FIELD = new ParseField("leader_shard_index");
    private static final ParseField LEADER_SHARD_INDEX_UUID_FIELD = new ParseField("leader_shard_index_uuid");
    private static final ParseField LEADER_SHARD_SHARDID_FIELD = new ParseField("leader_shard_shard");
    private static final ParseField HEADERS = new ParseField("headers");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ShardFollowTask, Void> PARSER = new ConstructingObjectParser<>(NAME,
            (a) -> new ShardFollowTask((String) a[0],
                new ShardId((String) a[1], (String) a[2], (int) a[3]), new ShardId((String) a[4], (String) a[5], (int) a[6]),
                (Integer) a[7], (Integer) a[8], (Integer) a[9], (Integer) a[10], (ByteSizeValue) a[11], (ByteSizeValue) a[12],
                (Integer) a[13], (ByteSizeValue) a[14], (TimeValue) a[15], (TimeValue) a[16], (Map<String, String>) a[17]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REMOTE_CLUSTER_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLLOW_SHARD_SHARDID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_SHARD_INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), LEADER_SHARD_SHARDID_FIELD);
        ImmutableFollowParameters.initParser(PARSER);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String remoteCluster;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final Map<String, String> headers;

    public ShardFollowTask(
            final String remoteCluster,
            final ShardId followShardId,
            final ShardId leaderShardId,
            final int maxReadRequestOperationCount,
            final int maxWriteRequestOperationCount,
            final int maxOutstandingReadRequests,
            final int maxOutstandingWriteRequests,
            final ByteSizeValue maxReadRequestSize,
            final ByteSizeValue maxWriteRequestSize,
            final int maxWriteBufferCount,
            final ByteSizeValue maxWriteBufferSize,
            final TimeValue maxRetryDelay,
            final TimeValue readPollTimeout,
            final Map<String, String> headers) {
        super(maxReadRequestOperationCount, maxWriteRequestOperationCount, maxOutstandingReadRequests, maxOutstandingWriteRequests,
            maxReadRequestSize, maxWriteRequestSize, maxWriteBufferCount, maxWriteBufferSize, maxRetryDelay, readPollTimeout);
        this.remoteCluster = remoteCluster;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public static ShardFollowTask readFrom(StreamInput in) throws IOException {
        String remoteCluster = in.readString();
        ShardId followShardId = new ShardId(in);
        ShardId leaderShardId = new ShardId(in);
        return new ShardFollowTask(remoteCluster, followShardId, leaderShardId, in);
    }

    private ShardFollowTask(String remoteCluster, ShardId followShardId, ShardId leaderShardId, StreamInput in) throws IOException {
        super(in);
        this.remoteCluster = remoteCluster;
        this.followShardId = followShardId;
        this.leaderShardId = leaderShardId;
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
        super.writeTo(out);
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
        toXContentFragment(builder);
        builder.field(HEADERS.getPreferredName(), headers);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ShardFollowTask that = (ShardFollowTask) o;
        return Objects.equals(remoteCluster, that.remoteCluster) &&
                Objects.equals(followShardId, that.followShardId) &&
                Objects.equals(leaderShardId, that.leaderShardId) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                remoteCluster,
                followShardId,
                leaderShardId,
                headers
        );
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }
}
