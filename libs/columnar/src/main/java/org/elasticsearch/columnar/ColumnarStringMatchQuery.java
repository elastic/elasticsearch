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
import java.util.function.Predicate;

/**
 * Documents whose keyword column holds a value {@code matcher} accepts, answered by the column rather than
 * by an inverted index.
 *
 * <p>For the shapes that are a test over values and nothing more: a set of terms, a range, a length. What the
 * column makes of that is its own business. A caller that knows its shape is a term or a prefix should say so
 * through {@link ColumnarStringTermQuery} instead, which can bisect; a caller holding an automaton should use
 * {@link ColumnarStringAutomatonQuery}, which can also hand its terms to a {@link QueryVisitor}.
 *
 * <p>{@code identity} stands in for the predicate in {@link #equals}, since a predicate has no equality worth
 * caching on: two queries carrying equal identities must accept the same values. It is whatever the caller
 * built the predicate from - a set of terms, the bounds of a range - so nothing has to be spelt into a string
 * to be compared, and a query over many terms does not carry a rendering of all of them for its lifetime.
 */
public final class ColumnarStringMatchQuery extends Query {

    private final String field;
    private final Predicate<BytesRef> matcher;
    private final Object identity;
    private final ScanBudget budget;

    public ColumnarStringMatchQuery(String field, Predicate<BytesRef> matcher, Object identity, ScanBudget budget) {
        this.field = Objects.requireNonNull(field);
        this.matcher = Objects.requireNonNull(matcher);
        this.identity = Objects.requireNonNull(identity);
        this.budget = Objects.requireNonNull(budget);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafReader reader = context.reader();
                final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
                if (info == null || info.getDocValuesType() != DocValuesType.BINARY) {
                    // No value for the field in this segment, so nothing here matches.
                    return null;
                }
                return new ConstantScoreScorerSupplier(score(), scoreMode, reader.maxDoc()) {
                    @Override
                    public long cost() {
                        return reader.maxDoc();
                    }

                    @Override
                    public DocIdSetIterator iterator(long leadCost) throws IOException {
                        // Checked here rather than where the supplier is built: everything below allocates, and
                        // the answer is not wanted until Lucene asks for the iterator.
                        budget.check(searcher);
                        final BinaryDocValues values = reader.getBinaryDocValues(field);
                        if (values == null) {
                            return DocIdSetIterator.empty();
                        }
                        if (values instanceof StringColumnSource columnar) {
                            return columnar.reader().match(matcher);
                        }

                        // An overlay rather than the column, as an updated field is: the values are read one
                        // document at a time and tested. The surface carries a document's slots as one payload, so
                        // each is tested in turn and any of them accepted accepts the document, which is what the
                        // column answers too.
                        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
                        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(values) {
                            @Override
                            public boolean matches() throws IOException {
                                final int slots = decoder.reset(values.binaryValue());
                                for (int slot = 0; slot < slots; slot++) {
                                    final BytesRef candidate = decoder.next();
                                    // A null is no value, so it is offered to no matcher — not even one that would
                                    // accept the empty string, which is a value a document really holds.
                                    if (candidate != null && matcher.test(candidate)) {
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
                };
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
        return "ColumnarStringMatchQuery(field=" + field + "," + identity + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        final ColumnarStringMatchQuery that = (ColumnarStringMatchQuery) other;
        return field.equals(that.field) && identity.equals(that.identity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, identity);
    }
}
