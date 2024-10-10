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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class LuceneOperator extends SourceOperator {
    private static final Logger logger = LogManager.getLogger(LuceneOperator.class);

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    protected final BlockFactory blockFactory;

    /**
     * Count of the number of slices processed.
     */
    private int processedSlices;
    final int maxPageSize;
    private final LuceneSliceQueue sliceQueue;

    private final Set<Query> processedQueries = new HashSet<>();
    private final Set<String> processedShards = new HashSet<>();

    private LuceneSlice currentSlice;
    private int sliceIndex;

    private LuceneScorer currentScorer;

    long processingNanos;
    int pagesEmitted;
    boolean doneCollecting;

    protected LuceneOperator(BlockFactory blockFactory, int maxPageSize, LuceneSliceQueue sliceQueue) {
        this.blockFactory = blockFactory;
        this.maxPageSize = maxPageSize;
        this.sliceQueue = sliceQueue;
    }

    public abstract static class Factory implements SourceOperator.SourceOperatorFactory {
        protected final DataPartitioning dataPartitioning;
        protected final int taskConcurrency;
        protected final int limit;
        protected final LuceneSliceQueue sliceQueue;

        /**
         * Build the factory.
         *
         * @param scoreMode the {@link ScoreMode} passed to {@link IndexSearcher#createWeight}
         */
        protected Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int limit,
            ScoreMode scoreMode
        ) {
            this.limit = limit;
            this.dataPartitioning = dataPartitioning;
            var weightFunction = weightFunction(queryFunction, scoreMode);
            this.sliceQueue = LuceneSliceQueue.create(contexts, weightFunction, dataPartitioning, taskConcurrency);
            this.taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        }

        public final int taskConcurrency() {
            return taskConcurrency;
        }

        public final int limit() {
            return limit;
        }
    }

    @Override
    public final Page getOutput() {
        try {
            return getCheckedOutput();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    protected abstract Page getCheckedOutput() throws IOException;

    @Override
    public void close() {}

    LuceneScorer getCurrentOrLoadNextScorer() {
        while (currentScorer == null || currentScorer.isDone()) {
            if (currentSlice == null || sliceIndex >= currentSlice.numLeaves()) {
                sliceIndex = 0;
                currentSlice = sliceQueue.nextSlice();
                if (currentSlice == null) {
                    doneCollecting = true;
                    return null;
                }
                processedSlices++;
                processedShards.add(currentSlice.shardContext().shardIdentifier());
            }
            final PartialLeafReaderContext partialLeaf = currentSlice.getLeaf(sliceIndex++);
            logger.trace("Starting {}", partialLeaf);
            final LeafReaderContext leaf = partialLeaf.leafReaderContext();
            if (currentScorer == null || currentScorer.leafReaderContext() != leaf) {
                final Weight weight = currentSlice.weight();
                processedQueries.add(weight.getQuery());
                currentScorer = new LuceneScorer(currentSlice.shardContext(), weight, leaf);
            }
            assert currentScorer.maxPosition <= partialLeaf.maxDoc() : currentScorer.maxPosition + ">" + partialLeaf.maxDoc();
            currentScorer.maxPosition = partialLeaf.maxDoc();
            currentScorer.position = Math.max(currentScorer.position, partialLeaf.minDoc());
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
        private final ShardContext shardContext;
        private final Weight weight;
        private final LeafReaderContext leafReaderContext;

        private BulkScorer bulkScorer;
        private int position;
        private int maxPosition;
        private Thread executingThread;

        LuceneScorer(ShardContext shardContext, Weight weight, LeafReaderContext leafReaderContext) {
            this.shardContext = shardContext;
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
            // avoid overflow and limit the range
            numDocs = Math.min(maxPosition - position, numDocs);
            assert numDocs > 0 : "scorer was exhausted";
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

        ShardContext shardContext() {
            return shardContext;
        }

        Weight weight() {
            return weight;
        }

        int position() {
            return position;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("maxPageSize = ").append(maxPageSize);
        describe(sb);
        sb.append("]");
        return sb.toString();
    }

    protected abstract void describe(StringBuilder sb);

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
        private final Set<String> processedQueries;
        private final Set<String> processedShards;
        private final long processingNanos;
        private final int totalSlices;
        private final int pagesEmitted;
        private final int sliceIndex;
        private final int sliceMin;
        private final int sliceMax;
        private final int current;

        private Status(LuceneOperator operator) {
            processedSlices = operator.processedSlices;
            processedQueries = operator.processedQueries.stream().map(Query::toString).collect(Collectors.toCollection(TreeSet::new));
            processingNanos = operator.processingNanos;
            processedShards = new TreeSet<>(operator.processedShards);
            sliceIndex = operator.sliceIndex;
            totalSlices = operator.sliceQueue.totalSlices();
            LuceneSlice slice = operator.currentSlice;
            if (slice != null && sliceIndex < slice.numLeaves()) {
                PartialLeafReaderContext leaf = slice.getLeaf(sliceIndex);
                sliceMin = leaf.minDoc();
                sliceMax = leaf.maxDoc();
            } else {
                sliceMin = 0;
                sliceMax = 0;
            }
            LuceneScorer scorer = operator.currentScorer;
            if (scorer == null) {
                current = 0;
            } else {
                current = scorer.position;
            }
            pagesEmitted = operator.pagesEmitted;
        }

        Status(
            int processedSlices,
            Set<String> processedQueries,
            Set<String> processedShards,
            long processingNanos,
            int sliceIndex,
            int totalSlices,
            int pagesEmitted,
            int sliceMin,
            int sliceMax,
            int current
        ) {
            this.processedSlices = processedSlices;
            this.processedQueries = processedQueries;
            this.processedShards = processedShards;
            this.processingNanos = processingNanos;
            this.sliceIndex = sliceIndex;
            this.totalSlices = totalSlices;
            this.pagesEmitted = pagesEmitted;
            this.sliceMin = sliceMin;
            this.sliceMax = sliceMax;
            this.current = current;
        }

        Status(StreamInput in) throws IOException {
            processedSlices = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                processedQueries = in.readCollectionAsSet(StreamInput::readString);
                processedShards = in.readCollectionAsSet(StreamInput::readString);
            } else {
                processedQueries = Collections.emptySet();
                processedShards = Collections.emptySet();
            }
            processingNanos = in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0;
            sliceIndex = in.readVInt();
            totalSlices = in.readVInt();
            pagesEmitted = in.readVInt();
            sliceMin = in.readVInt();
            sliceMax = in.readVInt();
            current = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(processedSlices);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeCollection(processedQueries, StreamOutput::writeString);
                out.writeCollection(processedShards, StreamOutput::writeString);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeVLong(processingNanos);
            }
            out.writeVInt(sliceIndex);
            out.writeVInt(totalSlices);
            out.writeVInt(pagesEmitted);
            out.writeVInt(sliceMin);
            out.writeVInt(sliceMax);
            out.writeVInt(current);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int processedSlices() {
            return processedSlices;
        }

        public Set<String> processedQueries() {
            return processedQueries;
        }

        public Set<String> processedShards() {
            return processedShards;
        }

        public long processNanos() {
            return processingNanos;
        }

        public int sliceIndex() {
            return sliceIndex;
        }

        public int totalSlices() {
            return totalSlices;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public int sliceMin() {
            return sliceMin;
        }

        public int sliceMax() {
            return sliceMax;
        }

        public int current() {
            return current;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("processed_slices", processedSlices);
            builder.field("processed_queries", processedQueries);
            builder.field("processed_shards", processedShards);
            builder.field("processing_nanos", processingNanos);
            if (builder.humanReadable()) {
                builder.field("processing_time", TimeValue.timeValueNanos(processingNanos));
            }
            builder.field("slice_index", sliceIndex);
            builder.field("total_slices", totalSlices);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("slice_min", sliceMin);
            builder.field("slice_max", sliceMax);
            builder.field("current", current);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return processedSlices == status.processedSlices
                && processedQueries.equals(status.processedQueries)
                && processedShards.equals(status.processedShards)
                && processingNanos == status.processingNanos
                && sliceIndex == status.sliceIndex
                && totalSlices == status.totalSlices
                && pagesEmitted == status.pagesEmitted
                && sliceMin == status.sliceMin
                && sliceMax == status.sliceMax
                && current == status.current;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processedSlices, sliceIndex, totalSlices, pagesEmitted, sliceMin, sliceMax, current);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }
    }

    static Function<ShardContext, Weight> weightFunction(Function<ShardContext, Query> queryFunction, ScoreMode scoreMode) {
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
