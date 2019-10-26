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

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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
    private String nodeId;
    private ShardId shardId;

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        shardId.writeTo(out);
        out.writeString(reason);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "SnapshotShardFailure{" +
            "shardId=" + shardId +
            ", reason='" + reason + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", status=" + status +
            '}';
    }

    static final ConstructingObjectParser<SnapshotShardFailure, Void> SNAPSHOT_SHARD_FAILURE_PARSER =
        new ConstructingObjectParser<>("shard_failure", true, SnapshotShardFailure::constructSnapshotShardFailure);

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

        ShardId shardId = new ShardId(index, indexUuid != null ? indexUuid : IndexMetaData.INDEX_UUID_NA_VALUE, intShardId);

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
        builder.field("index_uuid", shardId.getIndexName());
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
        // customized to account for discrepancies in shardId/Index toXContent/fromXContent related to uuid
        return shardId.id() == that.shardId.id() &&
            shardId.getIndexName().equals(shardId.getIndexName()) &&
            Objects.equals(reason, that.reason) &&
            Objects.equals(nodeId, that.nodeId) &&
            status.getStatus() == that.status.getStatus();
    }

    @Override
    public int hashCode() {
        // customized to account for discrepancies in shardId/Index toXContent/fromXContent related to uuid
        return Objects.hash(shardId.id(), shardId.getIndexName(), reason, nodeId, status.getStatus());
    }
}
