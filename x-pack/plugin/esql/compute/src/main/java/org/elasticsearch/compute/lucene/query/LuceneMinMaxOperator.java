/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Operator that finds the min or max value of a field using Lucene searches
 * and returns always one entry that mimics the min/max aggregation internal state:
 * 1. the min/max with a type depending on the {@link NumberType} (The initial value if no doc is seen)
 * 2. a bool flag (seen) that is true if at least one document has been matched, otherwise false
 * <p>
 * It works for fields that index data using lucene {@link PointValues} and/or {@link SortedNumericDocValues}.
 * It assumes that {@link SortedNumericDocValues} are always present.
 */
final class LuceneMinMaxOperator extends LuceneOperator {

    sealed interface NumberType permits LuceneMinFactory.NumberType, LuceneMaxFactory.NumberType {

        /** Extract the competitive value from the {@link PointValues}  */
        long fromPointValues(PointValues pointValues) throws IOException;

        /** Extract the competitive value from the segment-global {@link DocValuesSkipper} stats */
        long fromSkipper(DocValuesSkipper skipper);

        /**
         * Return true if the skipper's segment-global bound cannot improve {@code currentResult},
         * meaning the entire leaf can be skipped without scanning any documents.
         */
        boolean canSkipLeaf(DocValuesSkipper skipper, long currentResult);

        /** Wraps the provided {@link SortedNumericDocValues} with a {@link MultiValueMode} */
        LongValues multiValueMode(SortedNumericDocValues sortedNumericDocValues);

        /** Return the competitive value between {@code value1} and {@code value2} */
        long evaluate(long value1, long value2);

        /** Build the corresponding block */
        Block buildResult(BlockFactory blockFactory, long result, int pageSize);

        /** Build the corresponding block */
        Block buildEmptyResult(BlockFactory blockFactory, int pageSize);
    }

    private static final int PAGE_SIZE = 1;

    private boolean seen = false;
    private int remainingDocs;
    /** One running result per function, in the same order as {@link #numberTypes}. */
    private final long[] results;

    private final NumberType[] numberTypes;

    private final String fieldName;

    LuceneMinMaxOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        String fieldName,
        NumberType numberType,
        int limit,
        long initialResult,
        java.util.function.LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        this(
            shardRefCounters,
            driverContext,
            sliceQueue,
            fieldName,
            new NumberType[] { numberType },
            limit,
            new long[] { initialResult },
            directoryBytesRead,
            singleValueQueryWarnings
        );
    }

    /**
     * Computes several min/max functions over the same field in one pass. The output page holds, for each function in order,
     * the value block followed by the {@code seen} block.
     */
    LuceneMinMaxOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        String fieldName,
        NumberType[] numberTypes,
        int limit,
        long[] initialResults,
        java.util.function.LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(shardRefCounters, driverContext, PAGE_SIZE, sliceQueue, directoryBytesRead, singleValueQueryWarnings, false);
        assert numberTypes.length > 0 && numberTypes.length == initialResults.length;
        this.remainingDocs = limit;
        this.numberTypes = numberTypes;
        this.fieldName = fieldName;
        this.results = initialResults.clone();
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || remainingDocs <= 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    public Page getCheckedOutput() throws IOException {
        final long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            // no scorer means no more docs
            if (scorer != null) {
                if (scorer.tags().isEmpty() == false) {
                    throw new UnsupportedOperationException("tags not supported by " + getClass());
                }
                final LeafReader reader = scorer.leafReaderContext().reader();
                final Query query = scorer.weight().getQuery();
                final DocValuesSkipper skipper = reader.getDocValuesSkipper(fieldName);
                // The planner builds match-all as a boost(0) match_all, so look through the boost/constant-score wrappers.
                if (query == null || LuceneSourceOperator.Factory.unwrapQuery(query) instanceof MatchAllDocsQuery) {
                    final PointValues pointValues = reader.getPointValues(fieldName);
                    // Only apply the shortcut if we are visiting all documents: with a limit that is smaller than the number of
                    // documents we need to trigger the search on doc values, as that's the order they are visited without push down.
                    // Without a limit every document is visited, so the segment bounds always apply.
                    if (pointValues != null && (remainingDocs == NO_LIMIT || pointValues.getDocCount() >= remainingDocs)) {
                        final Bits liveDocs = reader.getLiveDocs();
                        if (liveDocs == null) {
                            // In data partitioning, we might have got the same segment previous
                            // to this but with a different document range. And we're totally ignoring that range.
                            // We're just reading the min/max from the segment. That's sneaky, but it makes sense.
                            // And if we get another slice in the same segment we may as well skip it -
                            // we've already looked.
                            if (scorer.position() == 0) {
                                seen = true;
                                for (int i = 0; i < numberTypes.length; i++) {
                                    results[i] = numberTypes[i].evaluate(results[i], numberTypes[i].fromPointValues(pointValues));
                                }
                                if (remainingDocs != NO_LIMIT) {
                                    remainingDocs -= pointValues.getDocCount();
                                }
                            }
                            scorer.markAsDone();
                        }
                    }
                    // Skipper fast path: covers fields with no points index, and the common no-limit case
                    // where the PointValues path above does not fire (getDocCount() < Integer.MAX_VALUE).
                    if (scorer.isDone() == false && skipper != null && skipper.docCount() > 0 && reader.getLiveDocs() == null) {
                        if (scorer.position() == 0) {
                            seen = true;
                            for (int i = 0; i < numberTypes.length; i++) {
                                results[i] = numberTypes[i].evaluate(results[i], numberTypes[i].fromSkipper(skipper));
                            }
                            if (remainingDocs != NO_LIMIT) {
                                remainingDocs -= skipper.docCount();
                            }
                        }
                        scorer.markAsDone();
                    }
                }
                // Leaf pruning: once we have a result, skip any segment whose skipper bound
                // cannot improve it, regardless of the query.
                if (scorer.isDone() == false && seen && skipper != null && canSkipLeaf(skipper)) {
                    scorer.markAsDone();
                }
                if (scorer.isDone() == false) {
                    // could not apply shortcut, trigger the search
                    // one doc values iterator per function: the multi value mode wrappers are stateful and must not share one
                    final LongValues[] values = new LongValues[numberTypes.length];
                    for (int i = 0; i < numberTypes.length; i++) {
                        values[i] = numberTypes[i].multiValueMode(DocValues.getSortedNumeric(reader, fieldName));
                    }
                    final LeafCollector leafCollector = new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) {}

                        @Override
                        public void collect(int doc) throws IOException {
                            assert remainingDocs > 0;
                            remainingDocs--;
                            for (int i = 0; i < values.length; i++) {
                                if (values[i].advanceExact(doc)) {
                                    seen = true;
                                    results[i] = numberTypes[i].evaluate(results[i], values[i].longValue());
                                }
                            }
                        }
                    };
                    scorer.scoreNextRange(leafCollector, reader.getLiveDocs(), remainingDocs);
                }
            }

            Page page = null;
            // emit only one page
            if (isFinished() && pagesEmitted == 0) {
                final Block[] blocks = new Block[numberTypes.length * 2];
                try {
                    for (int i = 0; i < numberTypes.length; i++) {
                        blocks[2 * i] = this.seen
                            ? numberTypes[i].buildResult(blockFactory, results[i], PAGE_SIZE)
                            : numberTypes[i].buildEmptyResult(blockFactory, PAGE_SIZE);
                        blocks[2 * i + 1] = blockFactory.newConstantBooleanBlockWith(this.seen, PAGE_SIZE);
                    }
                    page = new Page(PAGE_SIZE, blocks);
                } finally {
                    if (page == null) {
                        Releasables.closeExpectNoException(blocks);
                    }
                }
            }
            return page;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    /** A leaf can be skipped only when no function can improve its current result. */
    private boolean canSkipLeaf(DocValuesSkipper skipper) {
        for (int i = 0; i < numberTypes.length; i++) {
            if (numberTypes[i].canSkipLeaf(skipper, results[i]) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs=").append(remainingDocs);
    }
}
