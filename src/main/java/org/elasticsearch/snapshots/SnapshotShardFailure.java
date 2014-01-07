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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Stores information about failures that occurred during shard snapshotting process
 */
public class SnapshotShardFailure implements ShardOperationFailedException {
    private String index;

    private int shardId;

    private String reason;

    @Nullable
    private String nodeId;

    private RestStatus status;

    private SnapshotShardFailure() {

    }

    /**
     * Constructs new snapshot shard failure object
     *
     * @param nodeId  node where failure occurred
     * @param index   index which the shard belongs to
     * @param shardId shard id
     * @param reason  failure reason
     */
    public SnapshotShardFailure(@Nullable String nodeId, String index, int shardId, String reason) {
        this.nodeId = nodeId;
        this.index = index;
        this.shardId = shardId;
        this.reason = reason;
        status = RestStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * Returns index where failure occurred
     *
     * @return index
     */
    @Override
    public String index() {
        return this.index;
    }

    /**
     * Returns shard id where failure occurred
     *
     * @return shard id
     */
    @Override
    public int shardId() {
        return this.shardId;
    }

    /**
     * Returns reason for the failure
     *
     * @return reason for the failure
     */
    @Override
    public String reason() {
        return this.reason;
    }

    /**
     * Returns REST status corresponding to this failure
     *
     * @return REST status
     */
    @Override
    public RestStatus status() {
        return status;
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

    /**
     * Reads shard failure information from stream input
     *
     * @param in stream input
     * @return shard failure information
     * @throws IOException
     */
    public static SnapshotShardFailure readSnapshotShardFailure(StreamInput in) throws IOException {
        SnapshotShardFailure exp = new SnapshotShardFailure();
        exp.readFrom(in);
        return exp;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        nodeId = in.readOptionalString();
        index = in.readString();
        shardId = in.readVInt();
        reason = in.readString();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(reason);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "[" + index + "][" + shardId + "] failed, reason [" + reason + "]";
    }

    /**
     * Serializes snapshot failure information into JSON
     *
     * @param snapshotShardFailure snapshot failure information
     * @param builder              XContent builder
     * @param params               additional parameters
     * @throws IOException
     */
    public static void toXContent(SnapshotShardFailure snapshotShardFailure, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (snapshotShardFailure.nodeId != null) {
            builder.field("node_id", snapshotShardFailure.nodeId);
        }
        builder.field("index", snapshotShardFailure.index);
        builder.field("reason", snapshotShardFailure.reason);
        builder.field("shard_id", snapshotShardFailure.shardId);
        builder.field("status", snapshotShardFailure.status.name());
        builder.endObject();
    }

    /**
     * Deserializes snapshot failure information from JSON
     *
     * @param parser JSON parser
     * @return snapshot failure information
     * @throws IOException
     */
    public static SnapshotShardFailure fromXContent(XContentParser parser) throws IOException {
        SnapshotShardFailure snapshotShardFailure = new SnapshotShardFailure();

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            snapshotShardFailure.index = parser.text();
                        } else if ("node_id".equals(currentFieldName)) {
                            snapshotShardFailure.nodeId = parser.text();
                        } else if ("reason".equals(currentFieldName)) {
                            snapshotShardFailure.reason = parser.text();
                        } else if ("shard_id".equals(currentFieldName)) {
                            snapshotShardFailure.shardId = parser.intValue();
                        } else if ("status".equals(currentFieldName)) {
                            snapshotShardFailure.status = RestStatus.valueOf(parser.text());
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
        }
        return snapshotShardFailure;
    }
}
