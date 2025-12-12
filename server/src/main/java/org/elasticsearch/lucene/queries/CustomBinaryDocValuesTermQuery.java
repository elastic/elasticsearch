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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;
import java.util.Objects;

public final class CustomBinaryDocValuesTermQuery extends Query {

    private final String fieldName;
    private final BytesRef term;

    public CustomBinaryDocValuesTermQuery(String fieldName, BytesRef term) {
        this.fieldName = fieldName;
        this.term = term;
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

                    final ByteArrayStreamInput in = new ByteArrayStreamInput();
                    final BytesRef scratch = new BytesRef();

                    @Override
                    public boolean matches() throws IOException {
                        BytesRef binaryValue = values.binaryValue();
                        in.reset(binaryValue.bytes, binaryValue.offset, binaryValue.length);

                        int count = in.readVInt();
                        scratch.bytes = binaryValue.bytes;
                        for (int i = 0; i < count; i++) {
                            scratch.length = in.readVInt();
                            scratch.offset = in.getPosition();
                            if (term.equals(scratch)) {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 1; // because 1 comparisons
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
        return "CustomBinaryDocValuesTermQuery(fieldName=" + field + ",term=" + term.utf8ToString() + ")";
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
        CustomBinaryDocValuesTermQuery that = (CustomBinaryDocValuesTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
