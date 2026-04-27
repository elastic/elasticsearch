/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.*;

import org.apache.lucene.util.Bits;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.IdLoader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongPredicate;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Copied from Lucene's package-private {@code SortedNumericDocValuesRangeQuery} to allow
 * modification and instrumentation within Elasticsearch.
 */
public final class SortedNumericDocValuesRangeQuery extends NumericDocValuesRangeQuery {

    public SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
        super(field, lowerValue, upperValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
        return Objects.equals(field, that.field) && lowerValue == that.lowerValue && upperValue == that.upperValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, lowerValue, upperValue);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (this.field.equals(field) == false) {
            b.append(this.field).append(":");
        }
        return b.append("[").append(lowerValue).append(" TO ").append(upperValue).append("]").toString();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
            return new FieldExistsQuery(field);
        }
        if (lowerValue > upperValue) {
            return MatchNoDocsQuery.INSTANCE;
        }
        long globalMin = DocValuesSkipper.globalMinValue(indexSearcher.getIndexReader(), field);
        long globalMax = DocValuesSkipper.globalMaxValue(indexSearcher.getIndexReader(), field);
        if (lowerValue > globalMax || upperValue < globalMin) {
            return MatchNoDocsQuery.INSTANCE;
        }
        if (lowerValue <= globalMin
            && upperValue >= globalMax
            && DocValuesSkipper.globalDocCount(indexSearcher.getIndexReader(), field) == indexSearcher.getIndexReader().maxDoc()) {
            return MatchAllDocsQuery.INSTANCE;
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                    return null;
                }

                int maxDoc = context.reader().maxDoc();
                int count = docCountIgnoringDeletes(context);
                if (count == 0) {
                    return null;
                } else if (count == maxDoc) {
                    return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, maxDoc);
                }

                SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);
                final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                TwoPhaseIterator iterator;
                if (singleton != null && skipper != null) {
                    final DocIdSetIterator psIterator =
                        getDocIdSetIteratorOrNullForPrimarySort(context.reader(), singleton, skipper);
                    if (psIterator != null) {
                        return ConstantScoreScorerSupplier.fromIterator(psIterator, score(), scoreMode, maxDoc);
                    }
                }

                if (skipper != null && singleton instanceof BlockLoader.OptionalNumericRangeReader rangeReader) {
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return null;
                        }

                        @Override
                        public long cost() {
                            return singleton.cost();
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            return new BulkScorer() {

                                @Override
                                public int score(LeafCollector collector, Bits acceptDocs, int min, int maxExclusive) throws IOException {
                                    // make max inclusive
                                    int max = maxExclusive - 1;

                                    // want range [min, max)
//                                    System.out.println("min value: " + min + " max value: " + max);

                                    int currBlockStart = min;
                                    while (currBlockStart <= max) {
                                        // Also advances at beginning when value is -1
                                        if (skipper.maxDocID(0) < currBlockStart) {
                                            skipper.advance(currBlockStart);
                                            if (skipper.maxDocID(0) == NO_MORE_DOCS) {
                                                return NO_MORE_DOCS;
                                            }
                                        }

                                        int minDoc = skipper.minDocID(0);
                                        int maxDoc = skipper.maxDocID(0);
                                        int minDocInBlock = Math.max(min, minDoc);
                                        int maxDocInBlock = Math.min(maxDoc, max);

                                        long minVal = skipper.minValue(0);
                                        long maxVal = skipper.maxValue(0);

                                        if (lowerValue <= minVal && maxVal <= upperValue) {
                                            // Skipper range is entirely contained within query range
                                            // Collect all accepted Doc
                                            for (int doc = minDocInBlock; doc <= maxDocInBlock; doc++) {
                                                if (acceptDocs == null || acceptDocs.get(doc)) {
                                                    collector.collect(doc);
                                                }
                                            }
                                        } else if (minVal <= upperValue && lowerValue <= maxVal) {
                                            // Skipper range overlaps with query range
                                            // Collect accepted docs within [lowerValue, upperValue]
                                            rangeReader.tryCollectMatches(
                                                    collector,
                                                    acceptDocs,
                                                    minDocInBlock,
                                                    maxDocInBlock,
                                                    lowerValue,
                                                    upperValue
                                            );
                                        }
                                        // Else: Skipper block does not intersect range

                                        currBlockStart = maxDocInBlock + 1;
//                                        System.out.println("currBlockStart: " + currBlockStart);
                                    }
                                    return maxExclusive;
                                }

                                @Override
                                public long cost() {
                                    return singleton.cost() ;
                                }
                            };
                        }
                    };
                }

                if (skipper != null) {
                    // Use SkipBlockRangeIterator as the approximation: block-level skip
                    // filtering with no DV decoding. This exposes block skips to
                    // ConjunctionDISI so that when one field's block is NO, other fields
                    // never decode DV data for that block.
                    final SkipBlockRangeIterator skipApprox =
                        new SkipBlockRangeIterator(skipper, lowerValue, upperValue);




                    iterator =
                        new TwoPhaseIterator(skipApprox) {
                            private int cachedBlockEnd = -1;
                            private int cachedClassification = BLOCK_MAYBE;

                            @Override
                            public boolean matches() throws IOException {
                                int blockMatch = classifyBlockCached();
                                if (blockMatch == BLOCK_YES) {
                                    return true;
                                }
                                if (blockMatch == BLOCK_IF_DOC_HAS_VALUE) {
                                    if (singleton != null) {
                                        return singleton.advanceExact(skipApprox.docID());
                                    } else {
                                        return values.advanceExact(skipApprox.docID());
                                    }
                                }
                                // MAYBE — need to decode DV and check the actual value.
                                if (singleton != null) {
                                    if (singleton.advanceExact(skipApprox.docID())) {
                                        final long value = singleton.longValue();
                                        return value >= lowerValue && value <= upperValue;
                                    }
                                } else {
                                    if (values.advanceExact(skipApprox.docID())) {
                                        for (int i = 0, cnt = values.docValueCount(); i < cnt; ++i) {
                                            final long value = values.nextValue();
                                            if (value < lowerValue) {
                                                continue;
                                            }
                                            return value <= upperValue;
                                        }
                                    }
                                }
                                return false;
                            }

                            @Override
                            public int docIDRunEnd() throws IOException {
                                if (classifyBlockCached() == BLOCK_YES) {
                                    // Only report the current level-0 block as a run. The
                                    // approximation's docIDRunEnd() may expand to higher levels
                                    // that could be MAYBE, not YES.
                                    return cachedBlockEnd + 1;
                                }
                                return super.docIDRunEnd();
                            }

                            @Override
                            public float matchCost() {
                                return 3; // advanceExact + 2 comparisons
                            }

                            private static final int BLOCK_MAYBE = 0;
                            private static final int BLOCK_YES = 1;
                            private static final int BLOCK_IF_DOC_HAS_VALUE = 2;

                            private int classifyBlockCached() {
                                int blockEnd = skipper.maxDocID(0);
                                if (blockEnd != cachedBlockEnd) {
                                    cachedBlockEnd = blockEnd;
                                    cachedClassification = classifyBlock();
                                }
                                return cachedClassification;
                            }

                            private int classifyBlock() {
                                long blockMin = skipper.minValue(0);
                                long blockMax = skipper.maxValue(0);
                                if (blockMin >= lowerValue && blockMax <= upperValue) {
                                    if (skipper.docCount(0) == skipper.maxDocID(0) - skipper.minDocID(0) + 1) {
                                        return BLOCK_YES;
                                    }
                                    return BLOCK_IF_DOC_HAS_VALUE;
                                }
                                return BLOCK_MAYBE;
                            }
                        };
                } else if (singleton != null) {
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            final long value = singleton.longValue();
                            return value >= lowerValue && value <= upperValue;
                        }

                        @Override
                        public float matchCost() {
                            return 2;
                        }
                    };
                } else {
                    iterator = new TwoPhaseIterator(values) {
                        @Override
                        public boolean matches() throws IOException {
                            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                                final long value = values.nextValue();
                                if (value < lowerValue) {
                                    continue;
                                }
                                return value <= upperValue;
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 2;
                        }
                    };
                }
                return ConstantScoreScorerSupplier.fromIterator(
                    TwoPhaseIterator.asDocIdSetIterator(iterator),
                    score(),
                    scoreMode,
                    maxDoc
                );
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                int maxDoc = context.reader().maxDoc();
                int cnt = docCountIgnoringDeletes(context);
                if (cnt == maxDoc) {
                    return context.reader().numDocs();
                }
                return cnt;
            }

            private int docCountIgnoringDeletes(LeafReaderContext context) throws IOException {
                final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                if (skipper != null) {
                    if (skipper.minValue() > upperValue || skipper.maxValue() < lowerValue) {
                        return 0;
                    }
                    if (skipper.docCount() == context.reader().maxDoc()
                        && skipper.minValue() >= lowerValue
                        && skipper.maxValue() <= upperValue) {
                        return context.reader().maxDoc();
                    }
                }
                return -1;
            }
        };
    }

    private DocIdSetIterator getDocIdSetIteratorOrNullForPrimarySort(
        LeafReader reader,
        NumericDocValues numericDocValues,
        DocValuesSkipper skipper
    ) throws IOException {
        if (skipper.docCount() != reader.maxDoc()) {
            return null;
        }
        final Sort indexSort = reader.getMetaData().sort();
        if (indexSort == null
            || indexSort.getSort().length == 0
            || indexSort.getSort()[0].getField().equals(field) == false) {
            return null;
        }

        final int minDocID;
        final int maxDocID;
        if (indexSort.getSort()[0].getReverse()) {
            if (skipper.maxValue() <= upperValue) {
                minDocID = 0;
            } else {
                skipper.advance(Long.MIN_VALUE, upperValue);
                minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l <= upperValue);
            }
            if (skipper.minValue() >= lowerValue) {
                maxDocID = skipper.docCount();
            } else {
                skipper.advance(Long.MIN_VALUE, lowerValue);
                maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l < lowerValue);
            }
        } else {
            if (skipper.minValue() >= lowerValue) {
                minDocID = 0;
            } else {
                skipper.advance(lowerValue, Long.MAX_VALUE);
                minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l >= lowerValue);
            }
            if (skipper.maxValue() <= upperValue) {
                maxDocID = skipper.docCount();
            } else {
                skipper.advance(upperValue, Long.MAX_VALUE);
                maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l > upperValue);
            }
        }
        return minDocID == maxDocID ? DocIdSetIterator.empty() : DocIdSetIterator.range(minDocID, maxDocID);
    }

    private static int nextDoc(int startDoc, NumericDocValues docValues, LongPredicate predicate) throws IOException {
        int doc = docValues.docID();
        if (startDoc > doc) {
            doc = docValues.advance(startDoc);
        }
        for (; doc < NO_MORE_DOCS; doc = docValues.nextDoc()) {
            if (predicate.test(docValues.longValue())) {
                break;
            }
        }
        return doc;
    }
}
