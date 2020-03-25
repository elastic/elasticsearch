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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SnapshotIndexShardStatus extends BroadcastShardResponse implements ToXContentFragment {

    private SnapshotIndexShardStage stage = SnapshotIndexShardStage.INIT;

    private SnapshotStats stats;

    private String nodeId;

    private String failure;

    public SnapshotIndexShardStatus(StreamInput in) throws IOException {
        super(in);
        stage = SnapshotIndexShardStage.fromValue(in.readByte());
        stats = new SnapshotStats(in);
        nodeId = in.readOptionalString();
        failure = in.readOptionalString();
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(stage.value());
        stats.writeTo(out);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(failure);
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
        builder.field(SnapshotStats.Fields.STATS, stats, params);
        if (getNodeId() != null) {
            builder.field(Fields.NODE, getNodeId());
        }
        if (getFailure() != null) {
            builder.field(Fields.REASON, getFailure());
        }
        builder.endObject();
        return builder;
    }

    static final ObjectParser.NamedObjectParser<SnapshotIndexShardStatus, String> PARSER;
    static {
        ConstructingObjectParser<SnapshotIndexShardStatus, ShardId> innerParser = new ConstructingObjectParser<>(
            "snapshot_index_shard_status", true,
            (Object[] parsedObjects, ShardId shard) -> {
                int i = 0;
                String rawStage = (String) parsedObjects[i++];
                String nodeId = (String) parsedObjects[i++];
                String failure = (String) parsedObjects[i++];
                SnapshotStats stats = (SnapshotStats) parsedObjects[i];

                SnapshotIndexShardStage stage;
                try {
                    stage = SnapshotIndexShardStage.valueOf(rawStage);
                } catch (IllegalArgumentException iae) {
                    throw new ElasticsearchParseException(
                        "failed to parse snapshot index shard status [{}][{}], unknown stage [{}]",
                        shard.getIndex().getName(), shard.getId(), rawStage);
                }
                return new SnapshotIndexShardStatus(shard, stage, stats, nodeId, failure);
            }
        );
        innerParser.declareString(constructorArg(), new ParseField(Fields.STAGE));
        innerParser.declareString(optionalConstructorArg(), new ParseField(Fields.NODE));
        innerParser.declareString(optionalConstructorArg(), new ParseField(Fields.REASON));
        innerParser.declareObject(constructorArg(), (p, c) -> SnapshotStats.fromXContent(p), new ParseField(SnapshotStats.Fields.STATS));
        PARSER = (p, indexId, shardName) -> {
            // Combine the index name in the context with the shard name passed in for the named object parser
            // into a ShardId to pass as context for the inner parser.
            int shard;
            try {
                shard = Integer.parseInt(shardName);
            } catch (NumberFormatException nfe) {
                throw new ElasticsearchParseException(
                    "failed to parse snapshot index shard status [{}], expected numeric shard id but got [{}]", indexId, shardName);
            }
            ShardId shardId = new ShardId(new Index(indexId, IndexMetaData.INDEX_UUID_NA_VALUE), shard);
            return innerParser.parse(p, shardId);
        };
    }

    public static SnapshotIndexShardStatus fromXContent(XContentParser parser, String indexId) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        return PARSER.parse(parser, indexId, parser.currentName());
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
