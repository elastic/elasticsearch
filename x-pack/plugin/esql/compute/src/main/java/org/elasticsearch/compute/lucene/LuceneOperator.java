/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class LuceneOperator extends SourceOperator {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    private static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    private static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    @Nullable
    final IndexReader indexReader;
    final int shardId;
    @Nullable
    final Query query;
    final List<LuceneSourceOperator.PartialLeafReaderContext> leaves;
    final int maxPageSize;
    final int minPageSize;

    Weight weight;

    int currentLeaf = 0;
    LuceneSourceOperator.PartialLeafReaderContext currentLeafReaderContext = null;
    BulkScorer currentScorer = null;
    private Thread createdScorerThread = null;

    int currentPagePos;
    int currentScorerPos;
    int pagesEmitted;

    LuceneOperator(IndexReader reader, int shardId, Query query, int maxPageSize) {
        this.indexReader = reader;
        this.shardId = shardId;
        this.leaves = reader.leaves().stream().map(PartialLeafReaderContext::new).collect(Collectors.toList());
        this.query = query;
        this.maxPageSize = maxPageSize;
        this.minPageSize = Math.max(1, maxPageSize / 2);
    }

    LuceneOperator(Weight weight, int shardId, List<PartialLeafReaderContext> leaves, int maxPageSize) {
        this.indexReader = null;
        this.shardId = shardId;
        this.leaves = leaves;
        this.query = null;
        this.weight = weight;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
    }

    abstract LuceneOperator docSliceLuceneOperator(List<PartialLeafReaderContext> slice);

    abstract LuceneOperator segmentSliceLuceneOperator(IndexSearcher.LeafSlice leafSlice);

    public abstract static class LuceneOperatorFactory implements SourceOperatorFactory {

        final Function<SearchContext, Query> queryFunction;

        final DataPartitioning dataPartitioning;

        final int maxPageSize;

        final List<SearchContext> searchContexts;

        final int taskConcurrency;

        final int limit;

        private Iterator<LuceneOperator> iterator;

        public LuceneOperatorFactory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            this.searchContexts = searchContexts;
            this.queryFunction = queryFunction;
            this.dataPartitioning = dataPartitioning;
            this.taskConcurrency = taskConcurrency;
            this.maxPageSize = maxPageSize;
            this.limit = limit;
        }

        abstract LuceneOperator luceneOperatorForShard(int shardIndex);

        Iterator<LuceneOperator> sourceOperatorIterator() {
            final List<LuceneOperator> luceneOperators = new ArrayList<>();
            for (int shardIndex = 0; shardIndex < searchContexts.size(); shardIndex++) {
                LuceneOperator queryOperator = luceneOperatorForShard(shardIndex);
                switch (dataPartitioning) {
                    case SHARD -> luceneOperators.add(queryOperator);
                    case SEGMENT -> luceneOperators.addAll(queryOperator.segmentSlice());
                    case DOC -> luceneOperators.addAll(queryOperator.docSlice(taskConcurrency));
                    default -> throw new UnsupportedOperationException();
                }
            }
            return luceneOperators.iterator();
        }

        @Override
        public final SourceOperator get(DriverContext driverContext) {
            if (iterator == null) {
                iterator = sourceOperatorIterator();
            }
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                throw new IllegalStateException("Lucene operator factory exhausted");
            }
        }

        public int size() {
            return Math.toIntExact(
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(sourceOperatorIterator(), Spliterator.ORDERED), false).count()
            );
        }
    }

    /**
     * Split this source operator into a given number of slices
     */
    public List<LuceneOperator> docSlice(int numSlices) {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();

        List<LuceneOperator> operators = new ArrayList<>();
        for (List<PartialLeafReaderContext> slice : docSlices(indexReader, numSlices)) {
            operators.add(docSliceLuceneOperator(slice));
        }
        return operators;
    }

    static final List<List<PartialLeafReaderContext>> docSlices(IndexReader indexReader, int numSlices) {
        final int totalDocCount = indexReader.maxDoc();
        final int normalMaxDocsPerSlice = totalDocCount / numSlices;
        final int extraDocsInFirstSlice = totalDocCount % numSlices;
        final List<List<PartialLeafReaderContext>> slices = new ArrayList<>();
        int docsAllocatedInCurrentSlice = 0;
        List<PartialLeafReaderContext> currentSlice = null;
        int maxDocsPerSlice = normalMaxDocsPerSlice + extraDocsInFirstSlice;
        for (LeafReaderContext ctx : indexReader.leaves()) {
            final int numDocsInLeaf = ctx.reader().maxDoc();
            int minDoc = 0;
            while (minDoc < numDocsInLeaf) {
                int numDocsToUse = Math.min(maxDocsPerSlice - docsAllocatedInCurrentSlice, numDocsInLeaf - minDoc);
                if (numDocsToUse <= 0) {
                    break;
                }
                if (currentSlice == null) {
                    currentSlice = new ArrayList<>();
                }
                currentSlice.add(new PartialLeafReaderContext(ctx, minDoc, minDoc + numDocsToUse));
                minDoc += numDocsToUse;
                docsAllocatedInCurrentSlice += numDocsToUse;
                if (docsAllocatedInCurrentSlice == maxDocsPerSlice) {
                    slices.add(currentSlice);
                    maxDocsPerSlice = normalMaxDocsPerSlice; // once the first slice with the extra docs is added, no need for extra docs
                    currentSlice = null;
                    docsAllocatedInCurrentSlice = 0;
                }
            }
        }
        if (currentSlice != null) {
            slices.add(currentSlice);
        }
        if (numSlices < totalDocCount && slices.size() != numSlices) {
            throw new IllegalStateException("wrong number of slices, expected " + numSlices + " but got " + slices.size());
        }
        if (slices.stream()
            .flatMapToInt(
                l -> l.stream().mapToInt(partialLeafReaderContext -> partialLeafReaderContext.maxDoc - partialLeafReaderContext.minDoc)
            )
            .sum() != totalDocCount) {
            throw new IllegalStateException("wrong doc count");
        }
        return slices;
    }

    /**
     * Uses Lucene's own slicing method, which creates per-segment level slices
     */
    public List<LuceneOperator> segmentSlice() {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();
        List<LuceneOperator> operators = new ArrayList<>();
        for (IndexSearcher.LeafSlice leafSlice : segmentSlices(indexReader)) {
            operators.add(segmentSliceLuceneOperator(leafSlice));
        }
        return operators;
    }

    static IndexSearcher.LeafSlice[] segmentSlices(IndexReader indexReader) {
        return IndexSearcher.slices(indexReader.leaves(), MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException();
    }

    void initializeWeightIfNecessary() {
        if (weight == null) {
            try {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                weight = indexSearcher.createWeight(indexSearcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    boolean maybeReturnEarlyOrInitializeScorer() {
        // Reset the Scorer if the operator is run by a different thread
        if (currentLeafReaderContext != null && createdScorerThread != Thread.currentThread()) {
            try {
                currentScorer = weight.bulkScorer(currentLeafReaderContext.leafReaderContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            createdScorerThread = Thread.currentThread();
            return false;
        }
        if (currentLeafReaderContext == null) {
            assert currentScorer == null : "currentScorer wasn't reset";
            do {
                currentLeafReaderContext = leaves.get(currentLeaf);
                currentScorerPos = currentLeafReaderContext.minDoc;
                try {
                    currentScorer = weight.bulkScorer(currentLeafReaderContext.leafReaderContext);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (currentScorer == null) {
                    // doesn't match anything; move to the next leaf or abort if finished
                    currentLeaf++;
                    if (isFinished()) {
                        return true;
                    }
                }
            } while (currentScorer == null);
            createdScorerThread = Thread.currentThread();
        }
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("shardId=").append(shardId);
        sb.append("]");
        return sb.toString();
    }

    static class PartialLeafReaderContext {

        final LeafReaderContext leafReaderContext;
        final int minDoc; // incl
        final int maxDoc; // excl

        PartialLeafReaderContext(LeafReaderContext leafReaderContext, int minDoc, int maxDoc) {
            this.leafReaderContext = leafReaderContext;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
        }

        PartialLeafReaderContext(LeafReaderContext leafReaderContext) {
            this(leafReaderContext, 0, leafReaderContext.reader().maxDoc());
        }

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

        private final int currentLeaf;
        private final int totalLeaves;
        private final int pagesEmitted;
        private final int leafPosition;
        private final int leafSize;

        private Status(LuceneOperator operator) {
            currentLeaf = operator.currentLeaf;
            totalLeaves = operator.leaves.size();
            leafPosition = operator.currentScorerPos;
            LuceneOperator.PartialLeafReaderContext ctx = operator.currentLeafReaderContext;
            leafSize = ctx == null ? 0 : ctx.maxDoc - ctx.minDoc;
            pagesEmitted = operator.pagesEmitted;
        }

        Status(int currentLeaf, int totalLeaves, int pagesEmitted, int leafPosition, int leafSize) {
            this.currentLeaf = currentLeaf;
            this.totalLeaves = totalLeaves;
            this.leafPosition = leafPosition;
            this.leafSize = leafSize;
            this.pagesEmitted = pagesEmitted;
        }

        Status(StreamInput in) throws IOException {
            currentLeaf = in.readVInt();
            totalLeaves = in.readVInt();
            leafPosition = in.readVInt();
            leafSize = in.readVInt();
            pagesEmitted = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(currentLeaf);
            out.writeVInt(totalLeaves);
            out.writeVInt(leafPosition);
            out.writeVInt(leafSize);
            out.writeVInt(pagesEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int currentLeaf() {
            return currentLeaf;
        }

        public int totalLeaves() {
            return totalLeaves;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public int leafPosition() {
            return leafPosition;
        }

        public int leafSize() {
            return leafSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("current_leaf", currentLeaf);
            builder.field("total_leaves", totalLeaves);
            builder.field("leaf_position", leafPosition);
            builder.field("leaf_size", leafSize);
            builder.field("pages_emitted", pagesEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return currentLeaf == status.currentLeaf
                && totalLeaves == status.totalLeaves
                && pagesEmitted == status.pagesEmitted
                && leafPosition == status.leafPosition
                && leafSize == status.leafSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentLeaf, totalLeaves, pagesEmitted, leafPosition, leafSize);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
