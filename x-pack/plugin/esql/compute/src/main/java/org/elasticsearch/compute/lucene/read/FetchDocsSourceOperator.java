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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Emits the documents a fetch asks one shard for, as {@code _doc} pages for {@link ValuesSourceReaderOperator}. Each
 * page holds documents of a single segment: a constant shard, a constant segment and strictly increasing docs. That is
 * the shape the reader loads fastest, with one reader per page and no sorting or copying.
 */
public final class FetchDocsSourceOperator extends SourceOperator {
    /**
     * The documents to load from one shard, sorted by segment and then by doc, without duplicates. Rows come out in
     * exactly this order, so the caller can match them to its request. A violated order would load wrong values instead
     * of failing, so the constructor checks it. The arrays belong to this object once it is built, and nobody may
     * change them.
     *
     * @param shard the slot of the shard in {@link ShardDocsProvider#refCounteds()}
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

    /**
     * Hands out the shards of one fetch request, one per driver. The drivers of the request share it.
     */
    public interface ShardDocsProvider {
        /**
         * The shard the driver of {@code driverContext} loads, or {@code null} once every shard has a driver. Called
         * once per driver, from the thread that builds the drivers of the request.
         */
        @Nullable
        ShardDocs claim(DriverContext driverContext);

        /**
         * The shard references the pages take, by {@link ShardDocs#shard()}.
         */
        IndexedByShardId<? extends RefCounted> refCounteds();
    }

    /**
     * @param maxPageSize the most rows in one page. A segment run longer than that spans several pages.
     */
    public record Factory(ShardDocsProvider provider, int maxPageSize) implements SourceOperatorFactory {
        public Factory {
            checkMaxPageSize(maxPageSize);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new FetchDocsSourceOperator(
                driverContext.blockFactory(),
                provider.refCounteds(),
                provider.claim(driverContext),
                maxPageSize
            );
        }

        @Override
        public String describe() {
            return "FetchDocsSourceOperator[maxPageSize=" + maxPageSize + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final IndexedByShardId<? extends RefCounted> refCounteds;
    @Nullable
    private final ShardDocs shardDocs;
    private final int maxPageSize;

    private int cursor;
    private boolean finished;

    private int pagesEmitted;
    private long docsEmitted;
    private int segmentRuns;

    /**
     * @param shardDocs the documents to emit, {@code null} for a driver that got no shard
     */
    public FetchDocsSourceOperator(
        BlockFactory blockFactory,
        IndexedByShardId<? extends RefCounted> refCounteds,
        @Nullable ShardDocs shardDocs,
        int maxPageSize
    ) {
        this.blockFactory = blockFactory;
        this.refCounteds = refCounteds;
        this.shardDocs = shardDocs;
        this.maxPageSize = checkMaxPageSize(maxPageSize);
    }

    private static int checkMaxPageSize(int maxPageSize) {
        // an empty page would never move the cursor, and the driver would spin
        if (maxPageSize < 1) {
            throw new IllegalArgumentException("maxPageSize must be positive but was [" + maxPageSize + "]");
        }
        return maxPageSize;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished || shardDocs == null || cursor >= shardDocs.docCount();
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }
        int[] segments = shardDocs.segments();
        int segment = segments[cursor];
        int end = cursor;
        while (end < segments.length && segments[end] == segment && end - cursor < maxPageSize) {
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
        return "FetchDocsSourceOperator[maxPageSize=" + maxPageSize + "]";
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

        private static final TransportVersion ESQL_FETCH_PHASE = TransportVersion.fromName("esql_fetch_phase_plan");

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
            builder.startObject();
            builder.field("pages_emitted", pagesEmitted);
            builder.field("docs_emitted", docsEmitted);
            builder.field("segment_runs", segmentRuns);
            return builder.endObject();
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
            return ESQL_FETCH_PHASE;
        }
    }
}
