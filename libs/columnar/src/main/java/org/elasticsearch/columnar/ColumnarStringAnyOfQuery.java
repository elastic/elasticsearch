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
import java.util.Collection;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A query matching documents whose ColumNAR keyword column holds any value in the given term set.
 *
 * <p>On a {@link StringColumnSource} column this calls
 * {@link org.elasticsearch.columnar.string.StringColumnReader#matchAnyOf} directly, which:
 * bisects the values for a column in term order; bisects the dictionary for each query term in a
 * {@code DICTIONARY} column and tests a block of ordinals at a time; and compares bytes for a
 * {@code PLAIN} column. Non-source values are read one document at a time: a single-valued field's
 * blob is the value itself, and otherwise the slots are decoded from the binary payload.
 */
public final class ColumnarStringAnyOfQuery extends Query {

    private final String field;
    private final NavigableSet<BytesRef> terms;
    /** The same terms, for the paths that decide a value by lookup; built once here rather than per segment. */
    private final Set<BytesRef> membership;
    private final ScanBudget budget;

    /**
     * The terms in any order, sorted here. Taking a {@link Collection} rather than an ordered type is what
     * keeps a caller's own ordering out: the readers bisect these terms and test membership against a column
     * that is in byte order, and a sorted type handed in would carry its comparator into both.
     */
    public ColumnarStringAnyOfQuery(String field, Collection<BytesRef> terms, ScanBudget budget) {
        this.field = Objects.requireNonNull(field);
        this.terms = new TreeSet<>(Objects.requireNonNull(terms));
        this.membership = Set.copyOf(this.terms);
        this.budget = Objects.requireNonNull(budget);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (terms.isEmpty()) {
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
                            return columnar.reader().matchAnyOf(terms, membership);
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
                    return membership.contains(values.binaryValue());
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
                    if (candidate != null && membership.contains(candidate)) {
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

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return field + ":" + terms;
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        final ColumnarStringAnyOfQuery that = (ColumnarStringAnyOfQuery) other;
        return field.equals(that.field) && terms.equals(that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, terms);
    }
}
