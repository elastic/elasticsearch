/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Objects;

/**
 * A range query matching documents whose ColumNAR keyword column holds a value in
 * {@code [lower, upper]}. Bounds are optional; a null bound is open.
 *
 * <p>On a {@link StringColumnSource} column this calls
 * {@link org.elasticsearch.columnar.string.StringColumnReader#matchRange} directly, which:
 * bisects to a range of ranks when values arrive in term order; bisects the dictionary for a
 * {@code DICTIONARY} column and tests a block of ordinals at a time; and compares bytes for a
 * {@code PLAIN} column. Non-source values are read one document at a time: a single-valued field's
 * blob is the value itself, and otherwise the slots are decoded from the binary payload.
 */
public final class ColumnarStringRangeQuery extends Query {

    private final String field;
    /** Null means open lower bound. */
    private final BytesRef lower;
    /** Null means open upper bound. */
    private final BytesRef upper;
    private final boolean includeLower;
    private final boolean includeUpper;
    private final ScanBudget budget;

    public ColumnarStringRangeQuery(
        String field,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper,
        ScanBudget budget
    ) {
        this.field = Objects.requireNonNull(field);
        this.lower = lower == null ? null : BytesRef.deepCopyOf(lower);
        this.upper = upper == null ? null : BytesRef.deepCopyOf(upper);
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        this.budget = Objects.requireNonNull(budget);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (isEmptyRange()) {
                    return null;
                }
                final LeafReader reader = context.reader();
                final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
                if (info == null || info.getDocValuesType() != DocValuesType.BINARY) {
                    return null;
                }
                return new ConstantScoreScorerSupplier(score(), scoreMode, reader.maxDoc()) {
                    @Override
                    public long cost() {
                        return reader.maxDoc();
                    }

                    @Override
                    public DocIdSetIterator iterator(long leadCost) throws IOException {
                        budget.check(searcher);
                        final BinaryDocValues values = reader.getBinaryDocValues(field);
                        if (values == null) {
                            return DocIdSetIterator.empty();
                        }
                        if (values instanceof StringColumnSource columnar) {
                            return columnar.reader().matchRange(lower, includeLower, upper, includeUpper);
                        }
                        return fallbackIterator(values, ColumNARDocValuesFormat.isSingleValued(info));
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    private DocIdSetIterator fallbackIterator(BinaryDocValues values, boolean singleValued) {
        if (singleValued) {
            return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(values) {
                @Override
                public boolean matches() throws IOException {
                    return inRange(values.binaryValue());
                }

                @Override
                public float matchCost() {
                    return 100f;
                }
            });
        }
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(values) {
            @Override
            public boolean matches() throws IOException {
                final int slots = decoder.reset(values.binaryValue());
                for (int slot = 0; slot < slots; slot++) {
                    final BytesRef candidate = decoder.next();
                    if (candidate != null && inRange(candidate)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 100f;
            }
        });
    }

    private boolean inRange(BytesRef value) {
        if (lower != null) {
            final int cmp = value.compareTo(lower);
            if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                return false;
            }
        }
        if (upper != null) {
            final int cmp = value.compareTo(upper);
            if (cmp > 0 || (cmp == 0 && includeUpper == false)) {
                return false;
            }
        }
        return true;
    }

    private boolean isEmptyRange() {
        if (lower == null || upper == null) {
            return false;
        }
        final int cmp = lower.compareTo(upper);
        return cmp > 0 || (cmp == 0 && (includeLower == false || includeUpper == false));
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return field + ":" + (includeLower ? "[" : "{") + lower + " TO " + upper + (includeUpper ? "]" : "}");
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        final ColumnarStringRangeQuery that = (ColumnarStringRangeQuery) other;
        return field.equals(that.field)
            && includeLower == that.includeLower
            && includeUpper == that.includeUpper
            && Objects.equals(lower, that.lower)
            && Objects.equals(upper, that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, lower, upper, includeLower, includeUpper);
    }
}
