/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.segments;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndicesSegmentResponse extends BroadcastOperationResponse implements ToXContent {

    private ShardSegments[] shards;

    private Map<String, IndexSegments> indicesSegments;

    IndicesSegmentResponse() {

    }

    IndicesSegmentResponse(ShardSegments[] shards, ClusterState clusterState, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexSegments> getIndices() {
        return this.indices();
    }

    public Map<String, IndexSegments> indices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        Map<String, IndexSegments> indicesSegments = Maps.newHashMap();

        Set<String> indices = Sets.newHashSet();
        for (ShardSegments shard : shards) {
            indices.add(shard.index());
        }

        for (String index : indices) {
            List<ShardSegments> shards = Lists.newArrayList();
            for (ShardSegments shard : this.shards) {
                if (shard.shardRouting().index().equals(index)) {
                    shards.add(shard);
                }
            }
            indicesSegments.put(index, new IndexSegments(index, shards.toArray(new ShardSegments[shards.size()])));
        }
        this.indicesSegments = indicesSegments;
        return indicesSegments;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = new ShardSegments[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = ShardSegments.readShardSegments(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shards.length);
        for (ShardSegments shard : shards) {
            shard.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);

        for (IndexSegments indexSegments : indices().values()) {
            builder.startObject(indexSegments.index(), XContentBuilder.FieldCaseConversion.NONE);

            builder.startObject(Fields.SHARDS);
            for (IndexShardSegments indexSegment : indexSegments) {
                builder.startArray(Integer.toString(indexSegment.shardId().id()));
                for (ShardSegments shardSegments : indexSegment) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING);
                    builder.field(Fields.STATE, shardSegments.shardRouting().state());
                    builder.field(Fields.PRIMARY, shardSegments.shardRouting().primary());
                    builder.field(Fields.NODE, shardSegments.shardRouting().currentNodeId());
                    if (shardSegments.shardRouting().relocatingNodeId() != null) {
                        builder.field(Fields.RELOCATING_NODE, shardSegments.shardRouting().relocatingNodeId());
                    }
                    builder.endObject();

                    builder.field(Fields.NUM_COMMITTED_SEGMENTS, shardSegments.numberOfCommitted());
                    builder.field(Fields.NUM_SEARCH_SEGMENTS, shardSegments.numberOfSearch());

                    builder.startObject(Fields.SEGMENTS);
                    for (Segment segment : shardSegments) {
                        builder.startObject(segment.name());
                        builder.field(Fields.GENERATION, segment.generation());
                        builder.field(Fields.NUM_DOCS, segment.numDocs());
                        builder.field(Fields.DELETED_DOCS, segment.deletedDocs());
                        builder.field(Fields.SIZE, segment.size().toString());
                        builder.field(Fields.SIZE_IN_BYTES, segment.sizeInBytes());
                        builder.field(Fields.COMMITTED, segment.committed());
                        builder.field(Fields.SEARCH, segment.search());
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString STATE = new XContentBuilderString("state");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString RELOCATING_NODE = new XContentBuilderString("relocating_node");

        static final XContentBuilderString SEGMENTS = new XContentBuilderString("segments");
        static final XContentBuilderString GENERATION = new XContentBuilderString("generation");
        static final XContentBuilderString NUM_COMMITTED_SEGMENTS = new XContentBuilderString("num_committed_segments");
        static final XContentBuilderString NUM_SEARCH_SEGMENTS = new XContentBuilderString("num_search_segments");
        static final XContentBuilderString NUM_DOCS = new XContentBuilderString("num_docs");
        static final XContentBuilderString DELETED_DOCS = new XContentBuilderString("deleted_docs");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
        static final XContentBuilderString COMMITTED = new XContentBuilderString("committed");
        static final XContentBuilderString SEARCH = new XContentBuilderString("search");
    }
}