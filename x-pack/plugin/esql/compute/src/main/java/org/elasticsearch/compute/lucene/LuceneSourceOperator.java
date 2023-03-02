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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Source operator that incrementally runs Lucene searches
 */
@Experimental
public class LuceneSourceOperator extends SourceOperator {

    public static final int PAGE_SIZE = Math.toIntExact(ByteSizeValue.ofKb(16).getBytes());

    @Nullable
    private final IndexReader indexReader;
    private final int shardId;
    @Nullable
    private final Query query;
    private final List<PartialLeafReaderContext> leaves;
    private final int maxPageSize;
    private final int minPageSize;

    private Weight weight;

    private int currentLeaf = 0;
    private PartialLeafReaderContext currentLeafReaderContext = null;
    private BulkScorer currentScorer = null;

    private int currentPagePos;

    private IntVector.Builder currentBlockBuilder;

    private int currentScorerPos;
    private int pagesEmitted;

    private int numCollectedDocs = 0;
    private final int maxCollectedDocs;

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public static class LuceneSourceOperatorFactory implements SourceOperatorFactory {

        private final Function<SearchContext, Query> queryFunction;

        private final DataPartitioning dataPartitioning;

        private final int maxPageSize;

        private final List<SearchContext> searchContexts;

        private final int taskConcurrency;

        private final int limit;

        private Iterator<LuceneSourceOperator> iterator;

        public LuceneSourceOperatorFactory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int limit
        ) {
            this.searchContexts = searchContexts;
            this.queryFunction = queryFunction;
            this.dataPartitioning = dataPartitioning;
            this.taskConcurrency = taskConcurrency;
            this.maxPageSize = PAGE_SIZE;
            this.limit = limit;
        }

        @Override
        public SourceOperator get() {
            if (iterator == null) {
                iterator = sourceOperatorIterator();
            }
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                throw new IllegalStateException("Lucene source operator factory exhausted");
            }
        }

        private Iterator<LuceneSourceOperator> sourceOperatorIterator() {
            final List<LuceneSourceOperator> luceneOperators = new ArrayList<>();
            for (int shardIndex = 0; shardIndex < searchContexts.size(); shardIndex++) {
                final SearchContext ctx = searchContexts.get(shardIndex);
                final Query query = queryFunction.apply(ctx);
                final LuceneSourceOperator queryOperator = new LuceneSourceOperator(
                    ctx.getSearchExecutionContext().getIndexReader(),
                    shardIndex,
                    query,
                    maxPageSize,
                    limit
                );
                switch (dataPartitioning) {
                    case SHARD -> luceneOperators.add(queryOperator);
                    case SEGMENT -> luceneOperators.addAll(queryOperator.segmentSlice());
                    case DOC -> luceneOperators.addAll(queryOperator.docSlice(taskConcurrency));
                    default -> throw new UnsupportedOperationException();
                }
            }
            return luceneOperators.iterator();
        }

        public int size() {
            return Math.toIntExact(
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(sourceOperatorIterator(), Spliterator.ORDERED), false).count()
            );
        }

        @Override
        public String describe() {
            return "LuceneSourceOperator(dataPartitioning = " + dataPartitioning + ")";
        }
    }

    public LuceneSourceOperator(IndexReader reader, int shardId, Query query) {
        this(reader, shardId, query, PAGE_SIZE, NO_LIMIT);
    }

    public LuceneSourceOperator(IndexReader reader, int shardId, Query query, int maxPageSize, int limit) {
        this.indexReader = reader;
        this.shardId = shardId;
        this.leaves = reader.leaves().stream().map(PartialLeafReaderContext::new).collect(Collectors.toList());
        this.query = query;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
        currentBlockBuilder = IntVector.newVectorBuilder(maxPageSize);
        maxCollectedDocs = limit;
    }

    private LuceneSourceOperator(Weight weight, int shardId, List<PartialLeafReaderContext> leaves, int maxPageSize, int limit) {
        this.indexReader = null;
        this.shardId = shardId;
        this.leaves = leaves;
        this.query = null;
        this.weight = weight;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
        currentBlockBuilder = IntVector.newVectorBuilder(maxPageSize);
        maxCollectedDocs = limit;
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished() {
        return currentLeaf >= leaves.size() || numCollectedDocs >= maxCollectedDocs;
    }

    /**
     * Split this source operator into a given number of slices
     */
    public List<LuceneSourceOperator> docSlice(int numSlices) {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();

        List<LuceneSourceOperator> operators = new ArrayList<>();
        for (List<PartialLeafReaderContext> slice : docSlices(indexReader, numSlices)) {
            operators.add(new LuceneSourceOperator(weight, shardId, slice, maxPageSize, maxCollectedDocs));
        }
        return operators;
    }

    public static int numDocSlices(IndexReader indexReader, int numSlices) {
        return docSlices(indexReader, numSlices).size();
    }

    private static List<List<PartialLeafReaderContext>> docSlices(IndexReader indexReader, int numSlices) {
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
    public List<LuceneSourceOperator> segmentSlice() {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();
        List<LuceneSourceOperator> operators = new ArrayList<>();
        for (IndexSearcher.LeafSlice leafSlice : segmentSlices(indexReader)) {
            operators.add(
                new LuceneSourceOperator(
                    weight,
                    shardId,
                    Arrays.asList(leafSlice.leaves).stream().map(PartialLeafReaderContext::new).collect(Collectors.toList()),
                    maxPageSize,
                    maxCollectedDocs
                )
            );
        }
        return operators;
    }

    private static IndexSearcher.LeafSlice[] segmentSlices(IndexReader indexReader) {
        return IndexSearcher.slices(indexReader.leaves(), MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
    }

    public static int numSegmentSlices(IndexReader indexReader) {
        return segmentSlices(indexReader).length;
    }

    private static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    private static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }

        // initialize weight if not done yet
        initializeWeightIfNecessary();

        Page page = null;

        // initializes currentLeafReaderContext, currentScorer, and currentScorerPos when we switch to a new leaf reader
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
                        return null;
                    }
                }
            } while (currentScorer == null);
        }

        try {
            currentScorerPos = currentScorer.score(new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) {
                    if (numCollectedDocs < maxCollectedDocs) {
                        currentBlockBuilder.appendInt(doc);
                        numCollectedDocs++;
                        currentPagePos++;
                    }
                }
            },
                currentLeafReaderContext.leafReaderContext.reader().getLiveDocs(),
                currentScorerPos,
                // Note: if (maxPageSize - currentPagePos) is a small "remaining" interval, this could lead to slow collection with a
                // highly selective filter. Having a large "enough" difference between max- and minPageSize (and thus currentPagePos)
                // alleviates this issue.
                Math.min(currentLeafReaderContext.maxDoc, currentScorerPos + maxPageSize - currentPagePos)
            );

            if (currentPagePos >= minPageSize
                || currentScorerPos >= currentLeafReaderContext.maxDoc
                || numCollectedDocs >= maxCollectedDocs) {
                page = new Page(
                    currentPagePos,
                    new DocVector(
                        IntBlock.newConstantBlockWith(shardId, currentPagePos).asVector(),
                        IntBlock.newConstantBlockWith(currentLeafReaderContext.leafReaderContext.ord, currentPagePos).asVector(),
                        currentBlockBuilder.build(),
                        true
                    ).asBlock()
                );
                currentBlockBuilder = IntVector.newVectorBuilder(maxPageSize);
                currentPagePos = 0;
            }

            if (currentScorerPos >= currentLeafReaderContext.maxDoc) {
                currentLeaf++;
                currentLeafReaderContext = null;
                currentScorer = null;
                currentScorerPos = 0;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        pagesEmitted++;
        return page;
    }

    private void initializeWeightIfNecessary() {
        if (weight == null) {
            try {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                weight = indexSearcher.createWeight(indexSearcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
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

        private Status(LuceneSourceOperator operator) {
            currentLeaf = operator.currentLeaf;
            totalLeaves = operator.leaves.size();
            leafPosition = operator.currentScorerPos;
            PartialLeafReaderContext ctx = operator.currentLeafReaderContext;
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
