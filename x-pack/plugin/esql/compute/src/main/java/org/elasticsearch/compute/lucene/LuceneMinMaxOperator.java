/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
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

        /** Wraps the provided {@link SortedNumericDocValues} with a {@link MultiValueMode} */
        NumericDocValues multiValueMode(SortedNumericDocValues sortedNumericDocValues);

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
    private long result;

    private final NumberType numberType;

    private final String fieldName;

    LuceneMinMaxOperator(
        BlockFactory blockFactory,
        LuceneSliceQueue sliceQueue,
        String fieldName,
        NumberType numberType,
        int limit,
        long initialResult
    ) {
        super(blockFactory, PAGE_SIZE, sliceQueue);
        this.remainingDocs = limit;
        this.numberType = numberType;
        this.fieldName = fieldName;
        this.result = initialResult;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || remainingDocs == 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    public Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            assert remainingDocs <= 0 : remainingDocs;
            return null;
        }
        final long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            // no scorer means no more docs
            if (scorer == null) {
                remainingDocs = 0;
            } else {
                if (scorer.tags().isEmpty() == false) {
                    throw new UnsupportedOperationException("extra not supported by " + getClass());
                }
                final LeafReader reader = scorer.leafReaderContext().reader();
                final Query query = scorer.weight().getQuery();
                if (query == null || query instanceof MatchAllDocsQuery) {
                    final PointValues pointValues = reader.getPointValues(fieldName);
                    // only apply shortcut if we are visiting all documents, otherwise we need to trigger the search
                    // on doc values as that's the order they are visited without push down.
                    if (pointValues != null && pointValues.getDocCount() >= remainingDocs) {
                        final Bits liveDocs = reader.getLiveDocs();
                        if (liveDocs == null) {
                            // In data partitioning, we might have got the same segment previous
                            // to this but with a different document range. And we're totally ignoring that range.
                            // We're just reading the min/max from the segment. That's sneaky, but it makes sense.
                            // And if we get another slice in the same segment we may as well skip it -
                            // we've already looked.
                            if (scorer.position() == 0) {
                                seen = true;
                                result = numberType.evaluate(result, numberType.fromPointValues(pointValues));
                                if (remainingDocs != NO_LIMIT) {
                                    remainingDocs -= pointValues.getDocCount();
                                }
                            }
                            scorer.markAsDone();
                        }
                    }
                }
                if (scorer.isDone() == false) {
                    // could not apply shortcut, trigger the search
                    final NumericDocValues values = numberType.multiValueMode(reader.getSortedNumericDocValues(fieldName));
                    final LeafCollector leafCollector = new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) {}

                        @Override
                        public void collect(int doc) throws IOException {
                            assert remainingDocs > 0;
                            remainingDocs--;
                            if (values.advanceExact(doc)) {
                                seen = true;
                                result = numberType.evaluate(result, values.longValue());
                            }
                        }
                    };
                    scorer.scoreNextRange(leafCollector, reader.getLiveDocs(), remainingDocs);
                }
            }

            Page page = null;
            // emit only one page
            if (remainingDocs <= 0 && pagesEmitted == 0) {
                Block result = null;
                BooleanBlock seen = null;
                try {
                    result = this.seen
                        ? numberType.buildResult(blockFactory, this.result, PAGE_SIZE)
                        : numberType.buildEmptyResult(blockFactory, PAGE_SIZE);
                    seen = blockFactory.newConstantBooleanBlockWith(this.seen, PAGE_SIZE);
                    page = new Page(PAGE_SIZE, result, seen);
                } finally {
                    if (page == null) {
                        Releasables.closeExpectNoException(result, seen);
                    }
                }
            }
            return page;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs=").append(remainingDocs);
    }
}
