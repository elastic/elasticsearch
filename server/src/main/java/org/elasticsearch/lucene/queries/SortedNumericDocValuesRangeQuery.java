/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.LongPredicate;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Extends Lucene's package-private {@code SortedNumericDocValuesRangeQuery} to add
 * SIMD-based collect pushdown into TSDB codec blocks. Methods that are identical to Lucene's
 * implementation delegate to a stored instance of the Lucene query; only {@link
 * #createWeight(IndexSearcher, ScoreMode, float)} is overridden with ES-specific logic.
 */
public final class SortedNumericDocValuesRangeQuery extends NumericDocValuesRangeQuery {

    public static final FeatureFlag NUMERIC_RANGE_COLLECT_PUSHDOWN = new FeatureFlag("numeric_range_collect_pushdown");

    /**
     * Returns a range query over sorted numeric doc values for the given field.
     * When {@link #NUMERIC_RANGE_COLLECT_PUSHDOWN} is enabled, returns this Elasticsearch
     * subclass which supports SIMD-based collect pushdown into TSDB codec blocks.
     * When disabled, returns Lucene's own (package-private) implementation via
     * {@link SortedNumericDocValuesField#newSlowRangeQuery} to avoid shipping the
     * diverged copy of that code on the hot path.
     */
    public static Query newRangeQuery(String field, long lowerValue, long upperValue) {
        if (NUMERIC_RANGE_COLLECT_PUSHDOWN.isEnabled()) {
            return new SortedNumericDocValuesRangeQuery(field, lowerValue, upperValue);
        }
        return SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
    }

    public SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
        super(field, lowerValue, upperValue);
    }

    private final Query delegate = SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
        return delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return delegate.toString(field);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewritten = delegate.rewrite(indexSearcher);
        if (rewritten != delegate) {
            // Lucene simplified to MatchNoDocsQuery, MatchAllDocsQuery, or FieldExistsQuery.
            return rewritten;
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight delegateWeight = delegate.createWeight(searcher, scoreMode, boost);
        return new ConstantScoreWeight(this, boost) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                // 1) Preamble: early exits and primary sort optimization, copied from Lucene's
                // implementation. These must run before the TSDB path.
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
                if (singleton != null && skipper != null) {
                    final DocIdSetIterator psIterator = getDocIdSetIteratorOrNullForPrimarySort(context.reader(), singleton, skipper);
                    if (psIterator != null) {
                        return ConstantScoreScorerSupplier.fromIterator(psIterator, score(), scoreMode, maxDoc);
                    }
                }

                // 2) TSDB optimization: SIMD bitmask scanning over numeric codec blocks.
                if (singleton instanceof BlockLoader.OptionalNumericRangeReader rangeReader) {
                    var rangeIterator = rangeReader.tryRangeIterator(lowerValue, upperValue);
                    if (rangeIterator != null) {
                        return ConstantScoreScorerSupplier.fromIterator(rangeIterator, score(), scoreMode, maxDoc);
                    }
                }

                // 3) Delegate: fall back to Lucene's standard implementation.
                return delegateWeight.scorerSupplier(context);
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                return delegateWeight.count(context);
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
        if (indexSort == null || indexSort.getSort().length == 0 || indexSort.getSort()[0].getField().equals(field) == false) {
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
