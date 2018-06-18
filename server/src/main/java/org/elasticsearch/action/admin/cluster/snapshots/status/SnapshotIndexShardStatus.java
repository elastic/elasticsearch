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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;

import java.io.IOException;

public class SnapshotIndexShardStatus extends BroadcastShardResponse implements ToXContentFragment {

    private SnapshotIndexShardStage stage = SnapshotIndexShardStage.INIT;

    private SnapshotStats stats;

    private String nodeId;

    private String failure;

    private SnapshotIndexShardStatus() {
    }

    SnapshotIndexShardStatus(ShardId shardId, SnapshotIndexShardStage stage) {
        super(shardId);
        this.stage = stage;
        this.stats = new SnapshotStats();
    }

    SnapshotIndexShardStatus(ShardId shardId, IndexShardSnapshotStatus.Copy indexShardStatus) {
        this(shardId, indexShardStatus, null);
    }

    SnapshotIndexShardStatus(ShardId shardId, IndexShardSnapshotStatus.Copy indexShardStatus, String nodeId) {
        super(shardId);
        switch (indexShardStatus.getStage()) {
            case INIT:
                stage = SnapshotIndexShardStage.INIT;
                break;
            case STARTED:
                stage = SnapshotIndexShardStage.STARTED;
                break;
            case FINALIZE:
                stage = SnapshotIndexShardStage.FINALIZE;
                break;
            case DONE:
                stage = SnapshotIndexShardStage.DONE;
                break;
            case FAILURE:
                stage = SnapshotIndexShardStage.FAILURE;
                break;
            default:
                throw new IllegalArgumentException("Unknown stage type " + indexShardStatus.getStage());
        }
        this.stats = new SnapshotStats(indexShardStatus.getStartTime(), indexShardStatus.getTotalTime(),
            indexShardStatus.getIncrementalFileCount(), indexShardStatus.getTotalFileCount(), indexShardStatus.getProcessedFileCount(),
            indexShardStatus.getIncrementalSize(), indexShardStatus.getTotalSize(), indexShardStatus.getProcessedSize());
        this.failure = indexShardStatus.getFailure();
        this.nodeId = nodeId;
    }

    SnapshotIndexShardStatus(ShardId shardId, SnapshotIndexShardStage stage, SnapshotStats stats, String nodeId, String failure) {
        super(shardId);
        this.stage = stage;
        this.stats = stats;
        this.nodeId = nodeId;
        this.failure = failure;
    }

    /**
     * Returns snapshot stage
     */
    public SnapshotIndexShardStage getStage() {
        return stage;
    }

    /**
     * Returns snapshot stats
     */
    public SnapshotStats getStats() {
        return stats;
    }

    /**
     * Returns node id of the node where snapshot is currently running
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns reason for snapshot failure
     */
    public String getFailure() {
        return failure;
    }


    public static SnapshotIndexShardStatus readShardSnapshotStatus(StreamInput in) throws IOException {
        SnapshotIndexShardStatus shardStatus = new SnapshotIndexShardStatus();
        shardStatus.readFrom(in);
        return shardStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(stage.value());
        stats.writeTo(out);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(failure);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        stage = SnapshotIndexShardStage.fromValue(in.readByte());
        stats = SnapshotStats.readSnapshotStats(in);
        nodeId = in.readOptionalString();
        failure = in.readOptionalString();
    }

    static final class Fields {
        static final String STAGE = "stage";
        static final String REASON = "reason";
        static final String NODE = "node";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Integer.toString(getShardId().getId()));
        builder.field(Fields.STAGE, getStage());
        stats.toXContent(builder, params);
        if (getNodeId() != null) {
            builder.field(Fields.NODE, getNodeId());
        }
        if (getFailure() != null) {
            builder.field(Fields.REASON, getFailure());
        }
        builder.endObject();
        return builder;
    }

    // Todo: Standards check
    public static SnapshotIndexShardStatus fromXContent(XContentParser parser, String indexId) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String shardName = parser.currentName();
        int shard;
        try {
            shard = Integer.parseInt(shardName);
        } catch (NumberFormatException nfe) {
            throw new ElasticsearchParseException("failed to parse snapshot index shard status [{}], expected numeric shard id but got [{}]", indexId, shardName);
        }
        ShardId shardId = new ShardId(new Index(indexId, IndexMetaData.INDEX_UUID_NA_VALUE), shard);
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        SnapshotIndexShardStage stage = null;
        String nodeId = null;
        String failure = null;
        SnapshotStats stats = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token.equals(XContentParser.Token.FIELD_NAME)) {
                String currentName = parser.currentName();
                if (currentName.equals(Fields.STAGE)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser::getTokenLocation);
                    try {
                        stage = SnapshotIndexShardStage.valueOf(parser.text());
                    } catch (IllegalArgumentException iae) {
                        throw new ElasticsearchParseException("failed to parse snapshot index shard status [{}][{}], unknonwn stage [{}]", indexId, shardId.getId(), parser.text());
                    }
                } else if (currentName.equals(Fields.NODE)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser::getTokenLocation);
                    nodeId = parser.text();
                } else if (currentName.equals(Fields.REASON)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser::getTokenLocation);
                    failure = parser.text();
                } else if (currentName.equals(SnapshotStats.Fields.STATS)) {
                    stats = SnapshotStats.fromXContent(parser);
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshot index shard status [{}][{}], unknown field [{}]", indexId, shardId.getId(), currentName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshot index shard status [{}][{}]", indexId, shardId.getId());
            }
        }
        return new SnapshotIndexShardStatus(shardId, stage, stats, nodeId, failure);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotIndexShardStatus that = (SnapshotIndexShardStatus) o;

        if (stage != that.stage) return false;
        if (stats != null ? !stats.equals(that.stats) : that.stats != null) return false;
        if (nodeId != null ? !nodeId.equals(that.nodeId) : that.nodeId != null) return false;
        return failure != null ? failure.equals(that.failure) : that.failure == null;
    }

    @Override
    public int hashCode() {
        int result = stage != null ? stage.hashCode() : 0;
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
        result = 31 * result + (failure != null ? failure.hashCode() : 0);
        return result;
    }
}
