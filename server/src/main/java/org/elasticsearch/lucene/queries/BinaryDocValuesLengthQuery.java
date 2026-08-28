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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
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
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

final class BinaryDocValuesLengthQuery extends Query {

    final String fieldName;
    final int length;
    // Selects the decoder for the multi-valued fallback path; see BinaryDocValuesFormat.
    final BinaryDocValuesFormat format;

    BinaryDocValuesLengthQuery(String fieldName, int length, BinaryDocValuesFormat format) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.length = length;
        this.format = Objects.requireNonNull(format);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        // Captured for the binary doc values decode checkpoint below. This query is reached via rewrite() so it gets its own weight and
        // must establish the breaker itself.
        final CircuitBreaker breaker = ContextIndexSearcher.circuitBreakerOrNull(searcher);
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final FieldInfo fi = context.reader().getFieldInfos().fieldInfo(fieldName);
                if (fi == null || fi.getDocValuesType() != DocValuesType.BINARY) {
                    return null;
                }
                return new ConstantScoreScorerSupplier(score(), scoreMode, context.reader().maxDoc()) {
                    @Override
                    public long cost() {
                        return context.reader().maxDoc();
                    }

                    @Override
                    public DocIdSetIterator iterator(long leadCost) throws IOException {
                        // Checkpoint before opening: the probe is 0-byte heap sampling, so
                        // checking before the allocation skips it entirely when under pressure.
                        ContextIndexSearcher.checkBinaryDvDecodeBreaker(breaker);
                        final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                        if (values == null) {
                            return DocIdSetIterator.empty();
                        }

                        Predicate<BytesRef> lengthPredicate = bytes -> bytes.length == length;
                        if (format == BinaryDocValuesFormat.COLUMNAR_PAYLOAD) {
                            assert ColumnarBinaryDocValuesField.isColumnarStringPayload(context.reader(), fieldName)
                                : "field [" + fieldName + "] is mapped as a columnar payload but this segment does not carry one";
                            // The payload carries its own count; its blob is never a bare value, so no fast path applies.
                            return AbstractBinaryDocValuesQuery.columnarPayloadIterator(values, lengthPredicate, matchCost);
                        }
                        String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
                        final NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
                        DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
                        if ((countsSkipper == null || countsSkipper.maxValue() == 1)
                            && values instanceof BlockLoader.OptionalLengthReader direct) {
                            // tryLengthIterator returns a TwoPhaseIterator-backed iterator (see the contract on
                            // BlockLoader.OptionalLengthReader), so sub-segment slicing scales with cores.
                            return direct.tryLengthIterator(length);
                        }
                        if (format == BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL) {
                            return AbstractBinaryDocValuesQuery.arrayOrderInlineNullIterator(values, counts, lengthPredicate, matchCost);
                        } else if (countsSkipper != null) {
                            return AbstractBinaryDocValuesQuery.multiValuedIterator(values, counts, lengthPredicate, matchCost);
                        } else {
                            return AbstractBinaryDocValuesQuery.singleValuedIterator(values, lengthPredicate, matchCost);
                        }
                    }
                };
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
