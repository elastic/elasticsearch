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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Emits the slices a fetch driver takes from a {@link FetchSliceQueue}, as {@code _doc} pages for
 * {@link ValuesSourceReaderOperator}. Each page holds the documents of one slice: a constant shard, a constant segment and
 * strictly increasing docs. That is the shape the reader loads fastest, with one reader per page and no sorting or
 * copying. The second block of a page is the {@link FetchSliceQueue.Slice#position} of its slice, constant over the
 * page, so the rows of the slices that several drivers load can go back to request order.
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
    private final FetchSliceQueue slices;

    /**
     * The slice of the last page. The next page follows it, unless another driver took the slice after it.
     */
    @Nullable
    private FetchSliceQueue.Slice last;
    private boolean finished;

    private int pagesEmitted;
    private long docsEmitted;
    private int segmentRuns;

    /**
     * @param slices the slices of a request, which the sources of all its drivers share
     */
    public FetchDocsSourceOperator(BlockFactory blockFactory, IndexedByShardId<? extends RefCounted> refCounteds, FetchSliceQueue slices) {
        this.blockFactory = blockFactory;
        this.refCounteds = refCounteds;
        this.slices = slices;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }
        FetchSliceQueue.Slice slice = slices.nextSlice(last);
        if (slice == null) {
            finished = true;
            return null;
        }
        int positions = slice.docs().length;
        IntVector shard = null;
        IntVector segment = null;
        IntVector docs = null;
        Block position = null;
        Page page = null;
        try {
            shard = blockFactory.newConstantIntVector(slice.shard(), positions);
            segment = blockFactory.newConstantIntVector(slice.segment(), positions);
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
                for (int doc : slice.docs()) {
                    builder.appendInt(doc);
                }
                docs = builder.build();
            }
            position = blockFactory.newConstantIntBlockWith(slice.position(), positions);
            // the vector takes one reference on its shard and returns it when the page is released
            DocVector vector = new DocVector(refCounteds, shard, segment, docs, DocVector.config().singleSegmentNonDecreasing(true));
            page = new Page(positions, vector.asBlock(), position);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segment, docs, position);
            }
        }
        if (last == null || last.shard() != slice.shard() || last.segment() != slice.segment()) {
            segmentRuns++;
        }
        last = slice;
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
     * Pages, documents and segment runs a fetch driver emitted. A segment run starts with every page from another shard or
     * segment than the page before, so it counts how often the driver moved. The query phase already counted these
     * documents, so the totals of a profile don't count them again.
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
