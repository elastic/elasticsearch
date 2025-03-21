/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.ChunkedBroadcastResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IndicesSegmentResponse extends ChunkedBroadcastResponse {

    private final ShardSegments[] shards;

    private volatile Map<String, IndexSegments> indicesSegments;

    public IndicesSegmentResponse(
        ShardSegments[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexSegments> getIndices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        Map<String, IndexSegments> indicesSegments = new HashMap<>();

        final Map<String, List<ShardSegments>> segmentsByIndex = new HashMap<>();
        for (ShardSegments shard : shards) {
            segmentsByIndex.computeIfAbsent(shard.getShardRouting().getIndexName(), k -> new ArrayList<>()).add(shard);
        }
        for (Map.Entry<String, List<ShardSegments>> entry : segmentsByIndex.entrySet()) {
            indicesSegments.put(entry.getKey(), new IndexSegments(entry.getKey(), entry.getValue()));
        }
        this.indicesSegments = indicesSegments;
        return indicesSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected Iterator<ToXContent> customXContentChunks(ToXContent.Params params) {
        return ChunkedToXContentHelper.object(
            Fields.INDICES,
            Iterators.flatMap(
                getIndices().values().iterator(),
                indexSegments -> Iterators.concat(

                    ChunkedToXContentHelper.chunk((builder, p) -> builder.startObject(indexSegments.getIndex()).startObject(Fields.SHARDS)),
                    Iterators.flatMap(
                        indexSegments.iterator(),
                        indexSegment -> ChunkedToXContentHelper.array(
                            Integer.toString(indexSegment.shardId().id()),
                            Iterators.flatMap(
                                indexSegment.iterator(),
                                shardSegments -> Iterators.concat(

                                    ChunkedToXContentHelper.chunk((builder, p) -> {
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
                                        return builder;
                                    }),
                                    Iterators.flatMap(
                                        shardSegments.iterator(),
                                        segment -> Iterators.concat(

                                            ChunkedToXContentHelper.chunk((builder, p) -> {
                                                builder.startObject(segment.getName());
                                                builder.field(Fields.GENERATION, segment.getGeneration());
                                                builder.field(Fields.NUM_DOCS, segment.getNumDocs());
                                                builder.field(Fields.DELETED_DOCS, segment.getDeletedDocs());
                                                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, segment.getSize());
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
                                                return builder;
                                            }),
                                            getSegmentSortChunks(segment.getSegmentSort()),
                                            ChunkedToXContentHelper.chunk((builder, p) -> {
                                                if (segment.attributes != null && segment.attributes.isEmpty() == false) {
                                                    builder.field("attributes", segment.attributes);
                                                }
                                                builder.endObject();
                                                return builder;
                                            })
                                        )
                                    ),
                                    ChunkedToXContentHelper.chunk((builder, p) -> builder.endObject().endObject())
                                )
                            )
                        )
                    ),
                    ChunkedToXContentHelper.chunk((builder, p) -> builder.endObject().endObject())
                )
            )
        );
    }

    private static Iterator<ToXContent> getSegmentSortChunks(@Nullable Sort segmentSort) {
        if (segmentSort == null) {
            return Collections.emptyIterator();
        }

        return ChunkedToXContentHelper.array("sort", Iterators.map(Iterators.forArray(segmentSort.getSort()), field -> (builder, p) -> {
            builder.startObject();
            builder.field("field", field.getField());
            if (field instanceof SortedNumericSortField sortedNumericSortField) {
                builder.field("mode", sortedNumericSortField.getSelector().toString().toLowerCase(Locale.ROOT));
            } else if (field instanceof SortedSetSortField sortedSetSortField) {
                builder.field("mode", sortedSetSortField.getSelector().toString().toLowerCase(Locale.ROOT));
            }
            if (field.getMissingValue() != null) {
                builder.field("missing", field.getMissingValue().toString());
            }
            builder.field("reverse", field.getReverse());
            builder.endObject();
            return builder;
        }));
    }

    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";

        static final String SEGMENTS = "segments";
        static final String GENERATION = "generation";
        static final String NUM_COMMITTED_SEGMENTS = "num_committed_segments";
        static final String NUM_SEARCH_SEGMENTS = "num_search_segments";
        static final String NUM_DOCS = "num_docs";
        static final String DELETED_DOCS = "deleted_docs";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String COMMITTED = "committed";
        static final String SEARCH = "search";
        static final String VERSION = "version";
        static final String COMPOUND = "compound";
        static final String MERGE_ID = "merge_id";
        static final String MEMORY = "memory";
        static final String MEMORY_IN_BYTES = "memory_in_bytes";
    }
}
