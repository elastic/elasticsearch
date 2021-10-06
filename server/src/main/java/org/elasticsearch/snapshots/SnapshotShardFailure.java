/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Stores information about failures that occurred during shard snapshotting process
 */
public class SnapshotShardFailure extends ShardOperationFailedException {

    @Nullable
    private final String nodeId;
    private final ShardId shardId;

    SnapshotShardFailure(StreamInput in) throws IOException {
        nodeId = in.readOptionalString();
        shardId = new ShardId(in);
        super.shardId = shardId.getId();
        index = shardId.getIndexName();
        reason = in.readString();
        status = RestStatus.readFrom(in);
    }

    /**
     * Constructs new snapshot shard failure object
     *
     * @param nodeId  node where failure occurred
     * @param shardId shard id
     * @param reason  failure reason
     */
    public SnapshotShardFailure(@Nullable String nodeId, ShardId shardId, String reason) {
        this(nodeId, shardId, reason, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Constructs new snapshot shard failure object
     *
     * @param nodeId  node where failure occurred
     * @param shardId shard id
     * @param reason  failure reason
     * @param status  rest status
     */
    private SnapshotShardFailure(@Nullable String nodeId, ShardId shardId, String reason, RestStatus status) {
        super(shardId.getIndexName(), shardId.id(), reason, status, new IndexShardSnapshotFailedException(shardId, reason));
        this.nodeId = nodeId;
        this.shardId = shardId;
    }

    /**
     * Returns node id where failure occurred
     *
     * @return node id
     */
    @Nullable
    public String nodeId() {
        return nodeId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        shardId.writeTo(out);
        out.writeString(reason);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "SnapshotShardFailure{"
            + "shardId="
            + shardId
            + ", reason='"
            + reason
            + '\''
            + ", nodeId='"
            + nodeId
            + '\''
            + ", status="
            + status
            + '}';
    }

    static final ConstructingObjectParser<SnapshotShardFailure, Void> SNAPSHOT_SHARD_FAILURE_PARSER = new ConstructingObjectParser<>(
        "shard_failure",
        true,
        SnapshotShardFailure::constructSnapshotShardFailure
    );

    static {
        SNAPSHOT_SHARD_FAILURE_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("index"));
        SNAPSHOT_SHARD_FAILURE_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("index_uuid"));
        SNAPSHOT_SHARD_FAILURE_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("node_id"));
        // Workaround for https://github.com/elastic/elasticsearch/issues/25878
        // Some old snapshot might still have null in shard failure reasons
        SNAPSHOT_SHARD_FAILURE_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("reason"));
        SNAPSHOT_SHARD_FAILURE_PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("shard_id"));
        SNAPSHOT_SHARD_FAILURE_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("status"));
    }

    private static SnapshotShardFailure constructSnapshotShardFailure(Object[] args) {
        String index = (String) args[0];
        String indexUuid = (String) args[1];
        String nodeId = (String) args[2];
        String reason = (String) args[3];
        Integer intShardId = (Integer) args[4];
        String status = (String) args[5];

        if (index == null) {
            throw new ElasticsearchParseException("index name was not set");
        }
        if (intShardId == null) {
            throw new ElasticsearchParseException("index shard was not set");
        }

        ShardId shardId = new ShardId(index, indexUuid != null ? indexUuid : IndexMetadata.INDEX_UUID_NA_VALUE, intShardId);

        RestStatus restStatus;
        if (status != null) {
            restStatus = RestStatus.valueOf(status);
        } else {
            restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        }

        return new SnapshotShardFailure(nodeId, shardId, reason, restStatus);
    }

    /**
     * Deserializes snapshot failure information from JSON
     *
     * @param parser JSON parser
     * @return snapshot failure information
     */
    public static SnapshotShardFailure fromXContent(XContentParser parser) throws IOException {
        return SNAPSHOT_SHARD_FAILURE_PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", shardId.getIndexName());
        builder.field("index_uuid", shardId.getIndex().getUUID());
        builder.field("shard_id", shardId.id());
        builder.field("reason", reason);
        if (nodeId != null) {
            builder.field("node_id", nodeId);
        }
        builder.field("status", status.name());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotShardFailure that = (SnapshotShardFailure) o;
        return shardId.equals(that.shardId)
            && Objects.equals(reason, that.reason)
            && Objects.equals(nodeId, that.nodeId)
            && status.getStatus() == that.status.getStatus();
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, reason, nodeId, status.getStatus());
    }
}
