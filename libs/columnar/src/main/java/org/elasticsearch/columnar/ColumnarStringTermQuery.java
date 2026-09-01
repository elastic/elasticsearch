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
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Objects;

/**
 * Documents whose keyword column holds {@code term}, or a value starting with it or carrying it somewhere
 * inside, answered by the column rather than by an inverted index.
 *
 * <p>What that costs depends on the column's shape, which it decides for itself: a column in term order
 * bisects to the run of matches, a dictionary column matches over ordinals without reading a value, and
 * anything else compares the values a window at a time.
 *
 * <p>A caller gates on the format, so a field reaching this has a column. It is not always handed over as
 * one: an updated field is read as an overlay of its layers, which is no column, and then the values are
 * read a document at a time like any binary doc values.
 */
public final class ColumnarStringTermQuery extends Query {

    /** Where in a value the bytes have to sit for it to match. */
    private enum Where {
        WHOLE,
        START,
        ANYWHERE
    }

    private final String field;
    private final BytesRef term;
    private final Where where;

    /** Documents whose value is exactly {@code term}. */
    public static ColumnarStringTermQuery term(String field, BytesRef term) {
        return new ColumnarStringTermQuery(field, term, Where.WHOLE);
    }

    /** Documents holding a value that starts with {@code prefix}. */
    public static ColumnarStringTermQuery prefix(String field, BytesRef prefix) {
        return new ColumnarStringTermQuery(field, prefix, Where.START);
    }

    /** Documents holding a value that has {@code term} somewhere inside it. */
    public static ColumnarStringTermQuery contains(String field, BytesRef term) {
        return new ColumnarStringTermQuery(field, term, Where.ANYWHERE);
    }

    private ColumnarStringTermQuery(String field, BytesRef term, Where where) {
        this.field = Objects.requireNonNull(field);
        this.term = BytesRef.deepCopyOf(Objects.requireNonNull(term));
        this.where = where;
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
                    final DocIdSetIterator matches = switch (where) {
                        case WHOLE -> column.matchTerm(term);
                        case START -> column.matchPrefix(term);
                        case ANYWHERE -> column.matchContains(term);
                    };
                    return ConstantScoreScorerSupplier.fromIterator(matches, score(), scoreMode, reader.maxDoc());
                }

                // An overlay rather than the column, as an updated field is: the values are read one
                // document at a time and compared.
                final BytesRef value = new BytesRef();
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        final BytesRef candidate = values.binaryValue();
                        return switch (where) {
                            case WHOLE -> candidate.bytesEquals(term);
                            case START -> {
                                if (candidate.length < term.length) {
                                    yield false;
                                }
                                value.bytes = candidate.bytes;
                                value.offset = candidate.offset;
                                value.length = term.length;
                                yield value.bytesEquals(term);
                            }
                            case ANYWHERE -> ESVectorUtil.contains(
                                candidate.bytes,
                                candidate.offset,
                                candidate.length,
                                term.bytes,
                                term.offset,
                                term.length
                            );
                        };
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
        return switch (where) {
            case WHOLE -> field + ":" + term.utf8ToString();
            case START -> field + ":" + term.utf8ToString() + "*";
            case ANYWHERE -> field + ":*" + term.utf8ToString() + "*";
        };
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnarStringTermQuery q) {
            return where == q.where && field.equals(q.field) && term.bytesEquals(q.term);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, term, where);
    }
}
