/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.lucene.document.XSortedSkipperScorerSupplier;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongPredicate;

/**
 * Fork of Lucene 10.5.1's package-private {@code SortedSetDocValuesRangeQuery}, using
 * {@link XSortedSkipperScorerSupplier} so that {@code RangeBulkScorer} does not pass empty ranges
 * to {@code LeafCollector.collectRange} ({@code apache/lucene#16546}).
 *
 * <p>Delete this class, revert callers to {@link SortedSetDocValuesField#newSlowRangeQuery} and
 * {@link SortedSetDocValuesField#newSlowExactQuery}, and remove the {@link #newSlowRangeQuery} /
 * {@link #newSlowExactQuery} statics once Elasticsearch upgrades to a Lucene release containing
 * that fix.
 */
public final class XSortedSetDocValuesRangeQuery extends Query {

    public static Query newSlowRangeQuery(
        String field,
        BytesRef lowerValue,
        BytesRef upperValue,
        boolean lowerInclusive,
        boolean upperInclusive
    ) {
        return new XSortedSetDocValuesRangeQuery(field, lowerValue, upperValue, lowerInclusive, upperInclusive);
    }

    public static Query newSlowExactQuery(String field, BytesRef value) {
        return new XSortedSetDocValuesRangeQuery(field, value, value, true, true);
    }

    private final String field;
    private final BytesRef lowerValue;
    private final BytesRef upperValue;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    private XSortedSetDocValuesRangeQuery(
        String field,
        BytesRef lowerValue,
        BytesRef upperValue,
        boolean lowerInclusive,
        boolean upperInclusive
    ) {
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        this.lowerInclusive = lowerInclusive && lowerValue != null;
        this.upperInclusive = upperInclusive && upperValue != null;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        XSortedSetDocValuesRangeQuery that = (XSortedSetDocValuesRangeQuery) obj;
        return Objects.equals(field, that.field)
            && Objects.equals(lowerValue, that.lowerValue)
            && Objects.equals(upperValue, that.upperValue)
            && lowerInclusive == that.lowerInclusive
            && upperInclusive == that.upperInclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, lowerValue, upperValue, lowerInclusive, upperInclusive);
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
        return b.append(lowerInclusive ? "[" : "{")
            .append(lowerValue == null ? "*" : lowerValue)
            .append(" TO ")
            .append(upperValue == null ? "*" : upperValue)
            .append(upperInclusive ? "]" : "}")
            .toString();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (lowerValue == null && upperValue == null) {
            return new FieldExistsQuery(field);
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                    return null;
                }
                DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
                final SortedDocValues singleton = DocValues.unwrapSingleton(values);
                final SortField primarySortField;
                if (singleton != null && skipper != null && (primarySortField = densePrimarySort(context.reader(), skipper)) != null) {
                    return getScorerSupplierFromDensePrimarySort(singleton, values, skipper, primarySortField);
                }
                return new ConstantScoreScorerSupplier(score(), scoreMode, context.reader().maxDoc()) {
                    @Override
                    public DocIdSetIterator iterator(long leadCost) throws IOException {
                        final long minOrd = minOrd(values);
                        final long maxOrd = maxOrd(values);

                        if (minOrd > maxOrd || (skipper != null && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
                            return DocIdSetIterator.empty();
                        }

                        if (skipper != null
                            && skipper.docCount() == context.reader().maxDoc()
                            && skipper.minValue() >= minOrd
                            && skipper.maxValue() <= maxOrd) {
                            return DocIdSetIterator.all(skipper.docCount());
                        }

                        if (singleton != null) {
                            return TwoPhaseIterator.asDocIdSetIterator(
                                DocValuesRangeIterator.forOrdinalRange(singleton, skipper, minOrd, maxOrd)
                            );
                        }
                        return TwoPhaseIterator.asDocIdSetIterator(DocValuesRangeIterator.forOrdinalRange(values, skipper, minOrd, maxOrd));
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                    return 0;
                }
                SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
                final long minOrd = minOrd(values);
                final long maxOrd = maxOrd(values);
                if (minOrd > maxOrd) {
                    return 0;
                }
                final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                if (skipper != null) {
                    if (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()) {
                        return 0;
                    }
                    if (skipper.docCount() == context.reader().maxDoc() && skipper.minValue() >= minOrd && skipper.maxValue() <= maxOrd) {
                        return context.reader().numDocs();
                    }
                }
                return -1;
            }

            private ScorerSupplier getScorerSupplierFromDensePrimarySort(
                SortedDocValues singleton,
                SortedSetDocValues values,
                DocValuesSkipper skipper,
                SortField sortField
            ) {
                return new XSortedSkipperScorerSupplier(skipper, sortField, score(), scoreMode) {
                    long minOrd = -1;
                    long maxOrd = -1;

                    @Override
                    protected long getLowerValue() throws IOException {
                        if (minOrd == -1) {
                            minOrd = minOrd(values);
                        }
                        return minOrd;
                    }

                    @Override
                    protected long getUpperValue() throws IOException {
                        if (maxOrd == -1) {
                            maxOrd = maxOrd(values);
                        }
                        return maxOrd;
                    }

                    @Override
                    protected int nextDoc(int startDocId, LongPredicate predicate) throws IOException {
                        int doc = singleton.docID();
                        if (startDocId > doc) {
                            doc = singleton.advance(startDocId);
                        }
                        for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = singleton.nextDoc()) {
                            if (predicate.test(singleton.ordValue())) {
                                break;
                            }
                        }
                        return doc;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    private long minOrd(SortedSetDocValues values) throws IOException {
        if (lowerValue == null) {
            return 0;
        }
        final long ord = values.lookupTerm(lowerValue);
        if (ord < 0) {
            return -1 - ord;
        } else if (lowerInclusive) {
            return ord;
        } else {
            return ord + 1;
        }
    }

    private long maxOrd(SortedSetDocValues values) throws IOException {
        if (upperValue == null) {
            return values.getValueCount() - 1;
        }
        final long ord = values.lookupTerm(upperValue);
        if (ord < 0) {
            return -2 - ord;
        } else if (upperInclusive) {
            return ord;
        } else {
            return ord - 1;
        }
    }

    private SortField densePrimarySort(LeafReader reader, DocValuesSkipper skipper) {
        if (skipper.docCount() != reader.maxDoc()) {
            return null;
        }
        final Sort indexSort = reader.getMetaData().sort();
        if (indexSort == null || indexSort.getSort().length == 0 || indexSort.getSort()[0].getField().equals(field) == false) {
            return null;
        }
        return indexSort.getSort()[0];
    }
}
