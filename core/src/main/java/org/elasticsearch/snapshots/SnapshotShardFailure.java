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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Stores information about failures that occurred during shard snapshotting process
 */
public class SnapshotShardFailure implements ShardOperationFailedException {
    private ShardId shardId;

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
     * @param shardId shard id
     * @param reason  failure reason
     */
    public SnapshotShardFailure(@Nullable String nodeId, ShardId shardId, String reason) {
        this.nodeId = nodeId;
        this.shardId = shardId;
        this.reason = reason;
        assert reason != null;
        status = RestStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * Returns index where failure occurred
     *
     * @return index
     */
    @Override
    public String index() {
        return this.shardId.getIndexName();
    }

    /**
     * Returns shard id where failure occurred
     *
     * @return shard id
     */
    @Override
    public int shardId() {
        return this.shardId.id();
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

    @Override
    public Throwable getCause() {
        return new IndexShardSnapshotFailedException(shardId, reason);
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
     */
    public static SnapshotShardFailure readSnapshotShardFailure(StreamInput in) throws IOException {
        SnapshotShardFailure exp = new SnapshotShardFailure();
        exp.readFrom(in);
        return exp;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        nodeId = in.readOptionalString();
        shardId = ShardId.readShardId(in);
        reason = in.readString();
        status = RestStatus.readFrom(in);
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
        return shardId + " failed, reason [" + reason + "]";
    }

    /**
     * Serializes snapshot failure information into JSON
     *
     * @param snapshotShardFailure snapshot failure information
     * @param builder              XContent builder
     * @param params               additional parameters
     */
    public static void toXContent(SnapshotShardFailure snapshotShardFailure, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        snapshotShardFailure.toXContent(builder, params);
        builder.endObject();
    }

    /**
     * Deserializes snapshot failure information from JSON
     *
     * @param parser JSON parser
     * @return snapshot failure information
     */
    public static SnapshotShardFailure fromXContent(XContentParser parser) throws IOException {
        SnapshotShardFailure snapshotShardFailure = new SnapshotShardFailure();

        XContentParser.Token token = parser.currentToken();
        String index = null;
        String index_uuid = IndexMetaData.INDEX_UUID_NA_VALUE;
        int shardId = -1;
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            index = parser.text();
                        } else if ("index_uuid".equals(currentFieldName)) {
                            index_uuid = parser.text();
                        } else if ("node_id".equals(currentFieldName)) {
                            snapshotShardFailure.nodeId = parser.text();
                        } else if ("reason".equals(currentFieldName)) {
                            // Workaround for https://github.com/elastic/elasticsearch/issues/25878
                            // Some old snapshot might still have null in shard failure reasons
                            snapshotShardFailure.reason = parser.textOrNull();
                        } else if ("shard_id".equals(currentFieldName)) {
                            shardId = parser.intValue();
                        } else if ("status".equals(currentFieldName)) {
                            snapshotShardFailure.status = RestStatus.valueOf(parser.text());
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [{}]", currentFieldName);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token [{}]", token);
                }
            }
        } else {
            throw new ElasticsearchParseException("unexpected token [{}]", token);
        }
        if (index == null) {
            throw new ElasticsearchParseException("index name was not set");
        }
        if (shardId == -1) {
            throw new ElasticsearchParseException("index shard was not set");
        }
        snapshotShardFailure.shardId = new ShardId(index, index_uuid, shardId);
        // Workaround for https://github.com/elastic/elasticsearch/issues/25878
        // Some old snapshot might still have null in shard failure reasons
        if (snapshotShardFailure.reason == null) {
            snapshotShardFailure.reason = "";
        }
        return snapshotShardFailure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", shardId.getIndexName());
        builder.field("index_uuid", shardId.getIndexName());
        builder.field("shard_id", shardId.id());
        builder.field("reason", reason);
        if (nodeId != null) {
            builder.field("node_id", nodeId);
        }
        builder.field("status", status.name());
        return builder;
    }
}
