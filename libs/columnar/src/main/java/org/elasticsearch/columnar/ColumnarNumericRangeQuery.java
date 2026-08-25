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
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;

import java.io.IOException;
import java.util.Objects;

/**
 * A range query matching documents whose ColumNAR numeric value falls in {@code [lowerValue, upperValue]}.
 * It reads the column at the binary surface and drives its {@link ColumnarNumericBinaryDocValues#rangeIterator},
 * falling back to a per-document scan for sparse or multi-valued columns. Bounds are signed {@code long}s in
 * the column's stored encoding (sortable-long for doubles).
 */
public final class ColumnarNumericRangeQuery extends Query {

    private final String field;
    private final long lowerValue;
    private final long upperValue;

    public ColumnarNumericRangeQuery(String field, long lowerValue, long upperValue) {
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (lowerValue > upperValue) {
                    return null;
                }
                final LeafReader reader = context.reader();
                final BinaryDocValues values = reader.getBinaryDocValues(field);
                if (values == null) {
                    return null;
                }

                // Fast path: a dense single-valued column serves the vectorized, skipper-aware iterator.
                if (values instanceof ColumnarNumericBinaryDocValues column) {
                    final DocIdSetIterator iterator = column.rangeIterator(lowerValue, upperValue);
                    if (iterator != null) {
                        return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, reader.maxDoc());
                    }
                }

                // Fallback: a per-document scan of the decoded payloads, correct for every shape.
                final long[][] decoded = { new long[8] };
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        final int count = NumericBinaryPayload.decode(values.binaryValue(), decoded);
                        for (int i = 0; i < count; i++) {
                            final long value = decoded[0][i];
                            if (value >= lowerValue && value <= upperValue) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 2f;
                    }
                };
                return ConstantScoreScorerSupplier.fromIterator(
                    TwoPhaseIterator.asDocIdSetIterator(twoPhase),
                    score(),
                    scoreMode,
                    reader.maxDoc()
                );
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return field + ":[" + lowerValue + " TO " + upperValue + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnarNumericRangeQuery q) {
            return lowerValue == q.lowerValue && upperValue == q.upperValue && field.equals(q.field);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, lowerValue, upperValue);
    }
}
