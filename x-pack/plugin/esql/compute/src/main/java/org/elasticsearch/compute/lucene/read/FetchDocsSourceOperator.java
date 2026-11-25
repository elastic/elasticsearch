/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Emits the documents a fetch asks its shards for, as {@code _doc} pages for {@link ValuesSourceReaderOperator}. Each
 * page holds the documents of one segment of one shard: a constant shard, a constant segment and strictly increasing
 * docs. That is the shape the reader loads fastest, with one reader per page and no sorting or copying. A segment is
 * one page however many documents it holds, because the request already bounds them.
 */
public final class FetchDocsSourceOperator extends SourceOperator {
    /**
     * The documents to load from one shard, sorted by segment and then by doc, without duplicates. Rows come out in
     * exactly this order, so the caller can match them to its request. A violated order would load wrong values instead
     * of failing, so the constructor checks it. The arrays belong to this object once it is built, and nobody may
     * change them.
     *
     * @param shard the slot of the shard in the shard references the operator takes its pages' references from
     */
    public record ShardDocs(int shard, int[] segments, int[] docs) {
        public ShardDocs {
            checkSorted(segments, docs);
        }

        /**
         * Checks that {@code segments} and {@code docs} list documents sorted by segment and then by doc, without duplicates
         * and without negative values, the order every fetch keeps from the request to the loaded rows.
         *
         * @throws IllegalArgumentException when they don't
         */
        public static void checkSorted(int[] segments, int[] docs) {
            if (segments.length != docs.length) {
                throw new IllegalArgumentException("[" + segments.length + "] segments for [" + docs.length + "] docs");
            }
            for (int i = 0; i < docs.length; i++) {
                if (segments[i] < 0 || docs[i] < 0) {
                    throw new IllegalArgumentException("negative segment or doc at [" + i + "]");
                }
                if (i > 0) {
                    int cmp = Integer.compare(segments[i - 1], segments[i]);
                    if (cmp > 0 || (cmp == 0 && docs[i - 1] >= docs[i])) {
                        throw new IllegalArgumentException(
                            "documents must be sorted by segment and doc without duplicates but ["
                                + segments[i]
                                + ", "
                                + docs[i]
                                + "] follows ["
                                + segments[i - 1]
                                + ", "
                                + docs[i - 1]
                                + "]"
                        );
                    }
                }
            }
        }

        public int docCount() {
            return docs.length;
        }

        @Override
        public String toString() {
            return "ShardDocs[shard=" + shard + ", docs=" + docs.length + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final IndexedByShardId<? extends RefCounted> refCounteds;
    private final List<ShardDocs> shards;

    /**
     * The position in {@link #shards} of the shard the next page comes from.
     */
    private int shardIndex;
    private int cursor;
    private boolean finished;

    private int pagesEmitted;
    private long docsEmitted;
    private int segmentRuns;

    /**
     * Emits the documents of several shards, one shard after the other, so one driver loads them all.
     *
     * @param shards the documents to emit, in the order their rows come out
     */
    public FetchDocsSourceOperator(BlockFactory blockFactory, IndexedByShardId<? extends RefCounted> refCounteds, List<ShardDocs> shards) {
        this.blockFactory = blockFactory;
        this.refCounteds = refCounteds;
        this.shards = List.copyOf(shards);
        skipEmittedShards();
    }

    /**
     * Moves past the shards whose documents all went out, so {@link #shardIndex} names a shard with documents left, or the
     * end.
     */
    private void skipEmittedShards() {
        while (shardIndex < shards.size() && cursor >= shards.get(shardIndex).docCount()) {
            shardIndex++;
            cursor = 0;
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished || shardIndex >= shards.size();
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }
        ShardDocs shardDocs = shards.get(shardIndex);
        int[] segments = shardDocs.segments();
        int segment = segments[cursor];
        int end = cursor;
        while (end < segments.length && segments[end] == segment) {
            end++;
        }
        int positions = end - cursor;
        IntVector shard = null;
        IntVector segmentVector = null;
        IntVector docs = null;
        Page page = null;
        try {
            shard = blockFactory.newConstantIntVector(shardDocs.shard(), positions);
            segmentVector = blockFactory.newConstantIntVector(segment, positions);
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
                for (int p = cursor; p < end; p++) {
                    builder.appendInt(shardDocs.docs()[p]);
                }
                docs = builder.build();
            }
            // the vector takes one reference on its shard and returns it when the page is released
            DocVector vector = new DocVector(refCounteds, shard, segmentVector, docs, DocVector.config().singleSegmentNonDecreasing(true));
            page = new Page(positions, vector.asBlock());
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segmentVector, docs);
            }
        }
        if (cursor == 0 || segments[cursor - 1] != segment) {
            segmentRuns++;
        }
        cursor = end;
        skipEmittedShards();
        pagesEmitted++;
        docsEmitted += positions;
        return page;
    }

    @Override
    public void close() {}

    @Override
    public Status status() {
        return new Status(pagesEmitted, docsEmitted, segmentRuns);
    }

    @Override
    public String toString() {
        return "FetchDocsSourceOperator";
    }

    /**
     * Pages, documents and segment runs a fetch driver emitted. The query phase already counted these documents, so the
     * totals of a profile don't count them again.
     */
    public static final class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "fetch_docs_source",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(Status.class, (builder, value, params) -> {
            builder.startObject();
            builder.field("pages_emitted", value.pagesEmitted);
            builder.field("docs_emitted", value.docsEmitted);
            builder.field("segment_runs", value.segmentRuns);
            return builder.endObject();
        })
            .required("pages_emitted", ProfileSchema.integer())
            .required("docs_emitted", ProfileSchema.integer())
            .required("segment_runs", ProfileSchema.integer())
            .build();

        private static final TransportVersion ESQL_FETCH_DOCS_SOURCE_STATUS = TransportVersion.fromName("esql_fetch_docs_source_status");

        private final int pagesEmitted;
        private final long docsEmitted;
        private final int segmentRuns;

        public Status(int pagesEmitted, long docsEmitted, int segmentRuns) {
            this.pagesEmitted = pagesEmitted;
            this.docsEmitted = docsEmitted;
            this.segmentRuns = segmentRuns;
        }

        Status(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVLong(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesEmitted);
            out.writeVLong(docsEmitted);
            out.writeVInt(segmentRuns);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public long docsEmitted() {
            return docsEmitted;
        }

        public int segmentRuns() {
            return segmentRuns;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Status status = (Status) o;
            return pagesEmitted == status.pagesEmitted && docsEmitted == status.docsEmitted && segmentRuns == status.segmentRuns;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesEmitted, docsEmitted, segmentRuns);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // only nodes that run the fetch phase have fetch drivers to report
            return ESQL_FETCH_DOCS_SOURCE_STATUS;
        }
    }
}
