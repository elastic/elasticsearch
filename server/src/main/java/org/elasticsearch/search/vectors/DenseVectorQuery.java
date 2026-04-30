/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Exact knn query. Will iterate and score all documents that have the provided dense vector field in the index.
 * <p>
 * Each subclass takes an optional {@link VectorSimilarityFunction}, which selects one of three modes:
 * <ul>
 *     <li>{@code function == null}: scoring uses the codec-bound scorer returned by
 *     {@code vectorValues.scorer(query)}. On codec-quantized fields this scores against the
 *     quantized representation.</li>
 *     <li>{@code function != null} and equal to the field's bound similarity function: scoring uses
 *     {@code vectorValues.rescorer(query)}, Lucene's primitive for highest-fidelity raw scoring.
 *     For codecs that preserve raw vectors alongside quantized ones (INT8/INT4/BBQ via
 *     {@code QuantizedAndRawFloatVectorValues} and friends) this returns the raw scorer; for
 *     non-quantized codecs it equals {@code scorer(query)} per the Lucene default.</li>
 *     <li>{@code function != null} and different from the field's bound similarity function:
 *     scoring iterates {@code vectorValue(ord)} and applies {@code function.compare(target, raw)}
 *     directly — the only path that supports a per-query similarity-function override.</li>
 * </ul>
 */
public abstract class DenseVectorQuery extends Query {

    protected final String field;

    public DenseVectorQuery(String field) {
        this.field = field;
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    abstract static class DenseVectorWeight extends Weight {
        private final String field;
        private final float boost;

        protected DenseVectorWeight(DenseVectorQuery query, float boost) {
            super(query);
            this.field = query.field;
            this.boost = boost;
        }

        abstract VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException;

        @Override
        public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
            VectorScorer vectorScorer = vectorScorer(leafReaderContext);
            if (vectorScorer == null) {
                return Explanation.noMatch("No vector values found for field: " + field);
            }
            DocIdSetIterator iterator = vectorScorer.iterator();
            iterator.advance(i);
            if (iterator.docID() == i) {
                float score = vectorScorer.score();
                return Explanation.match(vectorScorer.score() * boost, "found vector with calculated similarity: " + score);
            }
            return Explanation.noMatch("Document not found in vector values for field: " + field);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            VectorScorer vectorScorer = vectorScorer(context);
            if (vectorScorer == null) {
                return null;
            }
            return new DefaultScorerSupplier(new DenseVectorScorer(vectorScorer, boost));
        }

        @Override
        public boolean isCacheable(LeafReaderContext leafReaderContext) {
            return true;
        }
    }

    public static class Floats extends DenseVectorQuery {

        private final float[] query;
        private final VectorSimilarityFunction function;

        /**
         * Codec-bound scoring (uses {@code FloatVectorValues.scorer(query)}). On quantized fields
         * this scores against the quantized representation.
         */
        public Floats(float[] query, String field) {
            this(query, field, null);
        }

        /**
         * Raw scoring with the given {@code function}. When {@code function} matches the field's bound
         * similarity, scoring uses {@link FloatVectorValues#rescorer(float[])} (Lucene's high-fidelity
         * raw primitive); when it differs, scoring iterates {@code vectorValue(ord)} and applies
         * {@code function.compare(query, raw)} directly. Pass {@code null} to use the codec scorer.
         */
        public Floats(float[] query, String field, VectorSimilarityFunction function) {
            super(field);
            this.query = query;
            this.function = function;
        }

        public float[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Floats";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new DenseVectorWeight(Floats.this, boost) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    if (function == null) {
                        return vectorValues.scorer(query);
                    }
                    FieldInfo fieldInfo = leafReaderContext.reader().getFieldInfos().fieldInfo(field);
                    if (fieldInfo != null && fieldInfo.getVectorSimilarityFunction() == function) {
                        return vectorValues.rescorer(query);
                    }
                    return new RawFloatVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats other = (Floats) o;
            return Objects.equals(field, other.field) && Objects.deepEquals(query, other.query) && function == other.function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function);
        }

        private static final class RawFloatVectorScorer implements VectorScorer {
            private final FloatVectorValues values;
            private final float[] target;
            private final VectorSimilarityFunction function;
            private final KnnVectorValues.DocIndexIterator iterator;

            RawFloatVectorScorer(FloatVectorValues values, float[] target, VectorSimilarityFunction function) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.iterator = values.iterator();
            }

            @Override
            public float score() throws IOException {
                return function.compare(target, values.vectorValue(iterator.index()));
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }
        }
    }

    public static class Bytes extends DenseVectorQuery {

        private final byte[] query;
        private final VectorSimilarityFunction function;

        /**
         * Codec-bound scoring (uses {@code ByteVectorValues.scorer(query)}).
         */
        public Bytes(byte[] query, String field) {
            this(query, field, null);
        }

        /**
         * Raw scoring with the given {@code function}. When {@code function} matches the field's bound
         * similarity, scoring uses {@link ByteVectorValues#rescorer(byte[])} (Lucene's high-fidelity
         * raw primitive); when it differs, scoring iterates {@code vectorValue(ord)} and applies
         * {@code function.compare(query, raw)} directly. Pass {@code null} to use the codec scorer.
         */
        public Bytes(byte[] query, String field, VectorSimilarityFunction function) {
            super(field);
            this.query = query;
            this.function = function;
        }

        public byte[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Bytes";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new DenseVectorWeight(Bytes.this, boost) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    ByteVectorValues vectorValues = leafReaderContext.reader().getByteVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    if (function == null) {
                        return vectorValues.scorer(query);
                    }
                    FieldInfo fieldInfo = leafReaderContext.reader().getFieldInfos().fieldInfo(field);
                    if (fieldInfo != null && fieldInfo.getVectorSimilarityFunction() == function) {
                        return vectorValues.rescorer(query);
                    }
                    return new RawByteVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes other = (Bytes) o;
            return Objects.equals(field, other.field) && Objects.deepEquals(query, other.query) && function == other.function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function);
        }

        private static final class RawByteVectorScorer implements VectorScorer {
            private final ByteVectorValues values;
            private final byte[] target;
            private final VectorSimilarityFunction function;
            private final KnnVectorValues.DocIndexIterator iterator;

            RawByteVectorScorer(ByteVectorValues values, byte[] target, VectorSimilarityFunction function) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.iterator = values.iterator();
            }

            @Override
            public float score() throws IOException {
                return function.compare(target, values.vectorValue(iterator.index()));
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }
        }
    }

    static class DenseVectorScorer extends Scorer {

        private final VectorScorer vectorScorer;
        private final DocIdSetIterator iterator;
        private final float boost;

        DenseVectorScorer(VectorScorer vectorScorer, float boost) {
            this.vectorScorer = vectorScorer;
            this.iterator = vectorScorer.iterator();
            this.boost = boost;
        }

        @Override
        public DocIdSetIterator iterator() {
            return vectorScorer.iterator();
        }

        @Override
        public float getMaxScore(int i) throws IOException {
            // TODO: can we optimize this at all?
            return Float.POSITIVE_INFINITY;
        }

        @Override
        public float score() throws IOException {
            assert iterator.docID() != -1;
            return vectorScorer.score() * boost;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }
    }

}
