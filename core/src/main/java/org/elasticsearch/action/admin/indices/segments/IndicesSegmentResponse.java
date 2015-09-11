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

package org.elasticsearch.action.admin.indices.segments;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndicesSegmentResponse extends BroadcastResponse implements ToXContent {

    private ShardSegments[] shards;

    private Map<String, IndexSegments> indicesSegments;

    IndicesSegmentResponse() {

    }

    IndicesSegmentResponse(ShardSegments[] shards, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexSegments> getIndices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        Map<String, IndexSegments> indicesSegments = new HashMap<>();

        Set<String> indices = new HashSet<>();
        for (ShardSegments shard : shards) {
            indices.add(shard.getShardRouting().getIndex());
        }

        for (String index : indices) {
            List<ShardSegments> shards = new ArrayList<>();
            for (ShardSegments shard : this.shards) {
                if (shard.getShardRouting().index().equals(index)) {
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

        for (IndexSegments indexSegments : getIndices().values()) {
            builder.startObject(indexSegments.getIndex(), XContentBuilder.FieldCaseConversion.NONE);

            builder.startObject(Fields.SHARDS);
            for (IndexShardSegments indexSegment : indexSegments) {
                builder.startArray(Integer.toString(indexSegment.getShardId().id()));
                for (ShardSegments shardSegments : indexSegment) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING);
                    builder.field(Fields.STATE, shardSegments.getShardRouting().state());
                    builder.field(Fields.PRIMARY, shardSegments.getShardRouting().primary());
                    builder.field(Fields.NODE, shardSegments.getShardRouting().currentNodeId());
                    if (shardSegments.getShardRouting().relocatingNodeId() != null) {
                        builder.field(Fields.RELOCATING_NODE, shardSegments.getShardRouting().relocatingNodeId());
                    }
                    builder.endObject();

                    builder.field(Fields.NUM_COMMITTED_SEGMENTS, shardSegments.getNumberOfCommitted());
                    builder.field(Fields.NUM_SEARCH_SEGMENTS, shardSegments.getNumberOfSearch());

                    builder.startObject(Fields.SEGMENTS);
                    for (Segment segment : shardSegments) {
                        builder.startObject(segment.getName());
                        builder.field(Fields.GENERATION, segment.getGeneration());
                        builder.field(Fields.NUM_DOCS, segment.getNumDocs());
                        builder.field(Fields.DELETED_DOCS, segment.getDeletedDocs());
                        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, segment.getSizeInBytes());
                        builder.byteSizeField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, segment.getMemoryInBytes());
                        builder.field(Fields.COMMITTED, segment.isCommitted());
                        builder.field(Fields.SEARCH, segment.isSearch());
                        if (segment.getVersion() != null) {
                            builder.field(Fields.VERSION, segment.getVersion());
                        }
                        if (segment.isCompound() != null) {
                            builder.field(Fields.COMPOUND, segment.isCompound());
                        }
                        if (segment.getMergeId() != null) {
                            builder.field(Fields.MERGE_ID, segment.getMergeId());
                        }
                        if (segment.ramTree != null) {
                            builder.startArray(Fields.RAM_TREE);
                            for (Accountable child : segment.ramTree.getChildResources()) {
                                toXContent(builder, child);
                            }
                            builder.endArray();
                        }
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
    
    static void toXContent(XContentBuilder builder, Accountable tree) throws IOException {
        builder.startObject();
        builder.field(Fields.DESCRIPTION, tree.toString());
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(tree.ramBytesUsed()));
        Collection<Accountable> children = tree.getChildResources();
        if (children.isEmpty() == false) {
            builder.startArray(Fields.CHILDREN);
            for (Accountable child : children) {
                toXContent(builder, child);
            }
            builder.endArray();
        }
        builder.endObject();
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
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString COMPOUND = new XContentBuilderString("compound");
        static final XContentBuilderString MERGE_ID = new XContentBuilderString("merge_id");
        static final XContentBuilderString MEMORY = new XContentBuilderString("memory");
        static final XContentBuilderString MEMORY_IN_BYTES = new XContentBuilderString("memory_in_bytes");
        static final XContentBuilderString RAM_TREE = new XContentBuilderString("ram_tree");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString CHILDREN = new XContentBuilderString("children");
    }
}