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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Objects;

/**
 * Documents whose keyword column holds {@code term}, or a value starting with it, answered by the column
 * rather than by an inverted index.
 *
 * <p>What that costs depends on the column's shape, which it decides for itself: a column in term order
 * bisects to the run of matches, a dictionary column matches over ordinals without reading a value, and
 * anything else compares the values a window at a time. A field this format did not write has no such
 * column, and the query declines it rather than falling back to a scan of its own.
 */
public final class ColumnarStringTermQuery extends Query {

    private final String field;
    private final BytesRef term;
    private final boolean prefix;

    /** Documents whose value is exactly {@code term}. */
    public static ColumnarStringTermQuery term(String field, BytesRef term) {
        return new ColumnarStringTermQuery(field, term, false);
    }

    /** Documents holding a value that starts with {@code prefix}. */
    public static ColumnarStringTermQuery prefix(String field, BytesRef prefix) {
        return new ColumnarStringTermQuery(field, prefix, true);
    }

    private ColumnarStringTermQuery(String field, BytesRef term, boolean prefix) {
        this.field = Objects.requireNonNull(field);
        this.term = BytesRef.deepCopyOf(Objects.requireNonNull(term));
        this.prefix = prefix;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafReader reader = context.reader();
                final BinaryDocValues values = reader.getBinaryDocValues(field);
                if (values == null) {
                    // No value for the field in this segment, so nothing here matches.
                    return null;
                }
                if (values instanceof StringColumnSource columnar) {
                    final StringColumnReader column = columnar.reader();
                    final DocIdSetIterator matches = prefix ? column.matchPrefix(term) : column.matchTerm(term);
                    return ConstantScoreScorerSupplier.fromIterator(matches, score(), scoreMode, reader.maxDoc());
                }

                // The column reached through something else, as an updated field is: its values are read one
                // document at a time and compared, which every binary doc values answers.
                final BytesRef value = new BytesRef();
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        final BytesRef candidate = values.binaryValue();
                        if (prefix) {
                            if (candidate.length < term.length) {
                                return false;
                            }
                            value.bytes = candidate.bytes;
                            value.offset = candidate.offset;
                            value.length = term.length;
                            return value.bytesEquals(term);
                        }
                        return candidate.bytesEquals(term);
                    }

                    @Override
                    public float matchCost() {
                        return 10f;
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
        return field + ":" + term.utf8ToString() + (prefix ? "*" : "");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnarStringTermQuery q) {
            return prefix == q.prefix && field.equals(q.field) && term.bytesEquals(q.term);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, term, prefix);
    }
}
