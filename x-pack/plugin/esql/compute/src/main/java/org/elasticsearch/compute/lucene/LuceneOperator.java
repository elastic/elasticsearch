/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;

public abstract class LuceneOperator extends SourceOperator {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    private int processSlices;
    final int maxPageSize;
    private final LuceneSliceQueue sliceQueue;

    private LuceneSlice currentSlice;
    private int sliceIndex;

    private LuceneScorer currentScorer;

    int pagesEmitted;
    boolean doneCollecting;

    public LuceneOperator(int maxPageSize, LuceneSliceQueue sliceQueue) {
        this.maxPageSize = maxPageSize;
        this.sliceQueue = sliceQueue;
    }

    public interface Factory extends SourceOperator.SourceOperatorFactory {
        int taskConcurrency();
    }

    @Override
    public void close() {

    }

    LuceneScorer getCurrentOrLoadNextScorer() {
        while (currentScorer == null || currentScorer.isDone()) {
            if (currentSlice == null || sliceIndex >= currentSlice.numLeaves()) {
                sliceIndex = 0;
                currentSlice = sliceQueue.nextSlice();
                if (currentSlice == null) {
                    doneCollecting = true;
                    return null;
                } else {
                    processSlices++;
                }
                if (currentSlice.numLeaves() == 0) {
                    continue;
                }
            }
            final PartialLeafReaderContext partialLeaf = currentSlice.getLeaf(sliceIndex++);
            final LeafReaderContext leaf = partialLeaf.leafReaderContext;
            if (currentScorer == null || currentScorer.leafReaderContext() != leaf) {
                final Weight weight = currentSlice.weight().get();
                currentScorer = new LuceneScorer(currentSlice.shardIndex(), currentSlice.searchContext(), weight, leaf);
            }
            assert currentScorer.maxPosition <= partialLeaf.maxDoc : currentScorer.maxPosition + ">" + partialLeaf.maxDoc;
            currentScorer.maxPosition = partialLeaf.maxDoc;
            currentScorer.position = Math.max(currentScorer.position, partialLeaf.minDoc);
        }
        if (Thread.currentThread() != currentScorer.executingThread) {
            currentScorer.reinitialize();
        }
        return currentScorer;
    }

    /**
     * Wraps a {@link BulkScorer} with shard information
     */
    static final class LuceneScorer {
        private final int shardIndex;
        private final SearchContext searchContext;
        private final Weight weight;
        private final LeafReaderContext leafReaderContext;

        private BulkScorer bulkScorer;
        private int position;
        private int maxPosition;
        private Thread executingThread;

        LuceneScorer(int shardIndex, SearchContext searchContext, Weight weight, LeafReaderContext leafReaderContext) {
            this.shardIndex = shardIndex;
            this.searchContext = searchContext;
            this.weight = weight;
            this.leafReaderContext = leafReaderContext;
            reinitialize();
        }

        private void reinitialize() {
            this.executingThread = Thread.currentThread();
            try {
                this.bulkScorer = weight.bulkScorer(leafReaderContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void scoreNextRange(LeafCollector collector, Bits acceptDocs, int numDocs) throws IOException {
            assert isDone() == false : "scorer is exhausted";
            position = bulkScorer.score(collector, acceptDocs, position, Math.min(maxPosition, position + numDocs));
        }

        LeafReaderContext leafReaderContext() {
            return leafReaderContext;
        }

        boolean isDone() {
            return bulkScorer == null || position >= maxPosition;
        }

        void markAsDone() {
            position = DocIdSetIterator.NO_MORE_DOCS;
        }

        int shardIndex() {
            return shardIndex;
        }

        SearchContext searchContext() {
            return searchContext;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append(", maxPageSize=").append(maxPageSize);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public Operator.Status status() {
        return new Status(this);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "lucene_source",
            Status::new
        );

        private final int processedSlices;
        private final int totalSlices;
        private final int pagesEmitted;
        private final int slicePosition;
        private final int sliceSize;

        private Status(LuceneOperator operator) {
            processedSlices = operator.processSlices;
            totalSlices = operator.sliceQueue.totalSlices();
            LuceneSlice slice = operator.currentSlice;
            final PartialLeafReaderContext leaf;
            int sliceIndex = operator.sliceIndex;
            if (slice != null && sliceIndex < slice.numLeaves()) {
                leaf = slice.getLeaf(sliceIndex);
            } else {
                leaf = null;
            }
            LuceneScorer scorer = operator.currentScorer;
            slicePosition = scorer != null ? scorer.position : 0;
            sliceSize = leaf != null ? leaf.maxDoc - leaf.minDoc : 0;
            pagesEmitted = operator.pagesEmitted;
        }

        Status(int processedSlices, int totalSlices, int pagesEmitted, int slicePosition, int sliceSize) {
            this.processedSlices = processedSlices;
            this.totalSlices = totalSlices;
            this.slicePosition = slicePosition;
            this.sliceSize = sliceSize;
            this.pagesEmitted = pagesEmitted;
        }

        Status(StreamInput in) throws IOException {
            processedSlices = in.readVInt();
            totalSlices = in.readVInt();
            slicePosition = in.readVInt();
            sliceSize = in.readVInt();
            pagesEmitted = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(processedSlices);
            out.writeVInt(totalSlices);
            out.writeVInt(slicePosition);
            out.writeVInt(sliceSize);
            out.writeVInt(pagesEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int currentLeaf() {
            return processedSlices;
        }

        public int totalLeaves() {
            return totalSlices;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public int leafPosition() {
            return slicePosition;
        }

        public int leafSize() {
            return sliceSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("processed_sliced", processedSlices);
            builder.field("total_slices", totalSlices);
            builder.field("slice_position", slicePosition);
            builder.field("slice_size", sliceSize);
            builder.field("pages_emitted", pagesEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return processedSlices == status.processedSlices
                && totalSlices == status.totalSlices
                && pagesEmitted == status.pagesEmitted
                && slicePosition == status.slicePosition
                && sliceSize == status.sliceSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processedSlices, totalSlices, pagesEmitted, slicePosition, sliceSize);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    static Function<SearchContext, Weight> weightFunction(Function<SearchContext, Query> queryFunction, ScoreMode scoreMode) {
        return ctx -> {
            final var query = queryFunction.apply(ctx);
            final var searcher = ctx.searcher();
            try {
                return searcher.createWeight(searcher.rewrite(new ConstantScoreQuery(query)), scoreMode, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
