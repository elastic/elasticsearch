/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose integer field value is present in a
 * {@link RoaringBitmap}. Checks bitmap membership via doc values using a
 * two-phase iterator.
 * Based on {@link org.apache.lucene.document.SortedNumericDocValuesSetQuery}.
 */
public class BitmapDocValuesQuery extends Query {
    private final String field;
    private final RoaringBitmap bitmap;
    private final long min;
    private final long max;

    public BitmapDocValuesQuery(String field, RoaringBitmap bitmap) {
        this.field = Objects.requireNonNull(field);
        this.bitmap = Objects.requireNonNull(bitmap);
        if (bitmap.isEmpty()) {
            this.min = 0;
            this.max = 0;
        } else {
            this.min = bitmap.first();
            this.max = bitmap.last();
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                    return null;
                }
                SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
                NumericDocValues singleton = DocValues.unwrapSingleton(values);
                final TwoPhaseIterator iterator;
                if (singleton != null) {
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            long value = singleton.longValue();
                            return value >= min && value <= max && bitmap.contains((int) value);
                        }

                        @Override
                        public float matchCost() {
                            return 8; // 2 comparisons + RoaringBitmap.contains (container lookup + probe)
                        }
                    };
                } else {
                    iterator = new TwoPhaseIterator(values) {
                        @Override
                        public boolean matches() throws IOException {
                            int count = values.docValueCount();
                            for (int i = 0; i < count; i++) {
                                final long value = values.nextValue();
                                if (value < min) {
                                    // sorted values: skip the below-min prefix
                                } else if (value > max) {
                                    return false; // values are sorted, terminate
                                } else if (bitmap.contains((int) value)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 8; // 2 comparisons + RoaringBitmap.contains (container lookup + probe)
                        }
                    };
                }
                final var scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
                return new DefaultScorerSupplier(scorer);
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
        if (bitmap.isEmpty()) {
            return "BitmapDocValuesQuery(field=" + field + ", cardinality=0)";
        }
        return "BitmapDocValuesQuery(field="
            + field
            + ", cardinality="
            + bitmap.getCardinality()
            + ", first="
            + bitmap.first()
            + ", last="
            + bitmap.last()
            + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapDocValuesQuery that = (BitmapDocValuesQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }
}
