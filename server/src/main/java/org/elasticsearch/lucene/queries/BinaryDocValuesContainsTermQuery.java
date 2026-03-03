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
import org.apache.lucene.index.NumericDocValues;
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
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

public final class BinaryDocValuesContainsTermQuery extends Query {
    final String fieldName;
    final BytesRef containsTerm;

    BinaryDocValuesContainsTermQuery(String fieldName, BytesRef containsTerm) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.containsTerm = Objects.requireNonNull(containsTerm);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
                final DocIdSetIterator iterator;

                // TODO This class would be simpler if it were made of subclass of AbstractBinaryDocValuesQuery, but we intend to
                //  optimize the single value case shortly with a `containsIterator` which pushes contain matching down to the codec.
                Predicate<BytesRef> predicate = bytes -> contains(bytes, containsTerm);
                iterator = TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
                    final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

                    @Override
                    public boolean matches() throws IOException {
                        values.advance(counts.docID());
                        return reader.match(values.binaryValue(), counts.longValue(), predicate);
                    }

                    @Override
                    public float matchCost() {
                        return matchCost;
                    }
                });

                return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, context.reader().maxDoc());
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    float matchCost() {
        return 10;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    public String toString(String field) {
        return "BinaryDocValuesContainsTermQuery(fieldName=" + field + ",containsTerm=" + containsTerm + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesContainsTermQuery that = (BinaryDocValuesContainsTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(containsTerm, that.containsTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, containsTerm);
    }

    static boolean contains(BytesRef value, BytesRef containsTerm) {
        int termEnd = containsTerm.offset + containsTerm.length;
        for (int i = value.offset, max = value.offset + value.length - containsTerm.length; i <= max; i++) {
            if (Arrays.mismatch(value.bytes, i, i + containsTerm.length, containsTerm.bytes, containsTerm.offset, termEnd) < 0) {
                return true;
            }
        }
        return false;
    }
}
