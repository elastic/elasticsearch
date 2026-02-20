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
import org.apache.lucene.index.DocValuesSkipper;
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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

final class BinaryDocValuesLengthQuery extends Query {

    final String fieldName;
    final int length;

    BinaryDocValuesLengthQuery(String fieldName, int length) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.length = length;
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

                String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
                final NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
                DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
                assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
                final DocIdSetIterator iterator;
                if (countsSkipper.maxValue() == 1 && values instanceof BlockLoader.OptionalLengthReader direct) {
                    iterator = direct.lengthIterator(length);
                } else {
                    Predicate<BytesRef> lengthPredicate = bytes -> bytes.length == length;
                    iterator = TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
                        final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

                        @Override
                        public boolean matches() throws IOException {
                            values.advance(counts.docID());
                            return reader.match(values.binaryValue(), counts.longValue(), lengthPredicate);
                        }

                        @Override
                        public float matchCost() {
                            return matchCost;
                        }
                    });
                }

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
        return "BinaryDocValuesLengthQuery(fieldName=" + field + ",length=" + length + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesLengthQuery that = (BinaryDocValuesLengthQuery) o;
        return Objects.equals(fieldName, that.fieldName) && length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, length);
    }

}
