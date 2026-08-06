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
import org.elasticsearch.lucene.search.XDocValuesRangeIterator;

import java.io.IOException;
import java.util.Objects;

/**
 * Fork of Lucene 10.5.0's package-private {@code SortedSetDocValuesRangeQuery}, using
 * {@link XDocValuesRangeIterator} so that {@code docIDRunEnd()} returns a correct, conservative
 * run boundary on {@code MAYBE} and {@code YES_IF_PRESENT} blocks.
 *
 * <p>The upstream fix is in apache/lucene#16450, which only landed in Lucene 11.0.0. Delete this
 * class, revert callers to {@link SortedSetDocValuesField#newSlowRangeQuery} and
 * {@link SortedSetDocValuesField#newSlowExactQuery}, and remove the {@link #newSlowRangeQuery} /
 * {@link #newSlowExactQuery} statics once Elasticsearch upgrades to a Lucene release containing
 * that fix.
 */
public final class SortedSetDocValuesRangeQuery extends Query {

    /**
     * Returns a range query equivalent to {@link SortedSetDocValuesField#newSlowRangeQuery} but
     * backed by the fixed {@link XDocValuesRangeIterator}.
     */
    public static Query newSlowRangeQuery(
        String field,
        BytesRef lowerValue,
        BytesRef upperValue,
        boolean lowerInclusive,
        boolean upperInclusive
    ) {
        return new SortedSetDocValuesRangeQuery(field, lowerValue, upperValue, lowerInclusive, upperInclusive);
    }

    /**
     * Returns an exact-match query equivalent to {@link SortedSetDocValuesField#newSlowExactQuery}
     * but backed by the fixed {@link XDocValuesRangeIterator}.
     */
    public static Query newSlowExactQuery(String field, BytesRef value) {
        return new SortedSetDocValuesRangeQuery(field, value, value, true, true);
    }

    private final String field;
    private final BytesRef lowerValue;
    private final BytesRef upperValue;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    // Used for rewrite(), count(), and the dense-primary-sort path; the dense-primary-sort scorer
    // supplier returns a DocIdSetIterator.range() and never calls docIDRunEnd(), so delegating that
    // path to Lucene is safe even without the apache/lucene#16450 fix.
    private final Query delegate;

    private SortedSetDocValuesRangeQuery(
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
        this.delegate = SortedSetDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue, lowerInclusive, upperInclusive);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SortedSetDocValuesRangeQuery that = (SortedSetDocValuesRangeQuery) obj;
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
        Weight delegateWeight = delegate.createWeight(searcher, scoreMode, boost);
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
                DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
                SortedDocValues singleton = DocValues.unwrapSingleton(values);

                // Dense primary sort path: the scorer supplier returns DocIdSetIterator.range()
                // directly and never calls docIDRunEnd(), so delegating to Lucene is safe here even
                // without the apache/lucene#16450 fix in our Lucene version.
                if (singleton != null && skipper != null && densePrimarySort(context.reader(), skipper) != null) {
                    return delegateWeight.scorerSupplier(context);
                }

                final long minOrd = minOrd(values);
                final long maxOrd = maxOrd(values);
                int maxDoc = context.reader().maxDoc();

                if (minOrd > maxOrd || (skipper != null && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
                    return null;
                }

                if (skipper != null && skipper.docCount() == maxDoc && skipper.minValue() >= minOrd && skipper.maxValue() <= maxOrd) {
                    return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, maxDoc);
                }

                // ConstantScoreScorerSupplier.fromIterator would set cost() = iterator.cost() =
                // SkipBlockRangeIterator.cost() = NO_MORE_DOCS (Lucene 10.5.0). Mirror Lucene's own
                // ordinal query which overrides cost() → values.cost() explicitly.
                final DocIdSetIterator disi = singleton != null
                    ? TwoPhaseIterator.asDocIdSetIterator(XDocValuesRangeIterator.forOrdinalRange(singleton, skipper, minOrd, maxOrd))
                    : TwoPhaseIterator.asDocIdSetIterator(XDocValuesRangeIterator.forOrdinalRange(values, skipper, minOrd, maxOrd));
                return new ConstantScoreScorerSupplier(score(), scoreMode, maxDoc) {
                    @Override
                    public DocIdSetIterator iterator(long leadCost) {
                        return disi;
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                return delegateWeight.count(context);
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
