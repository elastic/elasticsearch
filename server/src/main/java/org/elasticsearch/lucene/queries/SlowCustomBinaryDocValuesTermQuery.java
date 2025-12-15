/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.docvalues.CustomBinaryDocValuesReader;

import java.io.IOException;
import java.util.Objects;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equavalent of {@link org.apache.lucene.document.SortedDocValuesField#newSlowExactQuery(String, BytesRef)},
 * but then for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesTermQuery extends Query {

    private final String fieldName;
    private final BytesRef term;

    public SlowCustomBinaryDocValuesTermQuery(String fieldName, BytesRef term) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {

                    final CustomBinaryDocValuesReader reader = new CustomBinaryDocValuesReader();

                    @Override
                    public boolean matches() throws IOException {
                        BytesRef binaryValue = values.binaryValue();
                        return reader.match(binaryValue, term::equals);
                    }

                    @Override
                    public float matchCost() {
                        return 10; // because one comparison
                    }
                };

                return new DefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    @Override
    public String toString(String field) {
        return "SlowCustomBinaryDocValuesTermQuery(fieldName=" + field + ",term=" + term.utf8ToString() + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesTermQuery that = (SlowCustomBinaryDocValuesTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
