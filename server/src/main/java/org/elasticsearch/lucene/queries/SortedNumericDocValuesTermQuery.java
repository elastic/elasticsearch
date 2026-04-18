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
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongPredicate;

/**
 * A doc-values-only exact-match query for numeric fields, analogous to
 * {@link SortedNumericDocValuesRangeQuery} but specialized for a single value.
 * Uses {@link BlockLoader.OptionalNumericRangeReader#tryTermIterator} to push
 * filtering into the codec when available, avoiding the overhead of a
 * {@link TwoPhaseIterator}.
 */
public final class SortedNumericDocValuesTermQuery extends Query {

    private final String field;
    private final long value;

    public SortedNumericDocValuesTermQuery(String field, long value) {
        this.field = Objects.requireNonNull(field);
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SortedNumericDocValuesTermQuery that = (SortedNumericDocValuesTermQuery) obj;
        return Objects.equals(field, that.field) && value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, value);
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
        return b.append(value).toString();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        long globalMin = DocValuesSkipper.globalMinValue(indexSearcher.getIndexReader(), field);
        long globalMax = DocValuesSkipper.globalMaxValue(indexSearcher.getIndexReader(), field);
        if (value < globalMin || value > globalMax) {
            return MatchNoDocsQuery.INSTANCE;
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
                final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                if (skipper != null && (skipper.minValue() > value || skipper.maxValue() < value)) {
                    return null;
                }

                SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);

                TwoPhaseIterator iterator;
                if (singleton != null) {
                    if (skipper != null) {
                        final DocIdSetIterator psIterator = getDocIdSetIteratorOrNullForPrimarySort(
                            context.reader(),
                            singleton,
                            skipper
                        );
                        if (psIterator != null) {
                            return ConstantScoreScorerSupplier.fromIterator(psIterator, score(), scoreMode, maxDoc);
                        }
                    }
                    if (singleton instanceof BlockLoader.OptionalNumericRangeReader termReader) {
                        final DocIdSetIterator termIterator = termReader.tryTermIterator(value, skipper);
                        if (termIterator != null) {
                            return ConstantScoreScorerSupplier.fromIterator(termIterator, score(), scoreMode, maxDoc);
                        }
                    }
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            return singleton.longValue() == value;
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
                                if (values.nextValue() == value) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 2;
                        }
                    };
                }
                if (skipper != null) {
                    iterator = new DocValuesRangeIterator(iterator, skipper, value, value, false);
                }
                return ConstantScoreScorerSupplier.fromIterator(
                    TwoPhaseIterator.asDocIdSetIterator(iterator),
                    score(),
                    scoreMode,
                    maxDoc
                );
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
            if (skipper.maxValue() <= value) {
                minDocID = 0;
            } else {
                skipper.advance(Long.MIN_VALUE, value);
                minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l <= value);
            }
            if (skipper.minValue() >= value) {
                maxDocID = skipper.docCount();
            } else {
                skipper.advance(Long.MIN_VALUE, value);
                maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l < value);
            }
        } else {
            if (skipper.minValue() >= value) {
                minDocID = 0;
            } else {
                skipper.advance(value, Long.MAX_VALUE);
                minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l >= value);
            }
            if (skipper.maxValue() <= value) {
                maxDocID = skipper.docCount();
            } else {
                skipper.advance(value, Long.MAX_VALUE);
                maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l > value);
            }
        }
        return minDocID == maxDocID ? DocIdSetIterator.empty() : DocIdSetIterator.range(minDocID, maxDocID);
    }

    private static int nextDoc(int startDoc, NumericDocValues docValues, LongPredicate predicate) throws IOException {
        int doc = docValues.docID();
        if (startDoc > doc) {
            doc = docValues.advance(startDoc);
        }
        for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = docValues.nextDoc()) {
            if (predicate.test(docValues.longValue())) {
                break;
            }
        }
        return doc;
    }
}
