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

    /**
     * Scores using the codec-bound {@link VectorScorer}. For codec-quantized fields (INT8_*, INT4_*, BBQ_*),
     * this scores against the quantized representation rather than the raw vectors. Use {@link RawFloats}
     * to force scoring against the original full-precision vectors regardless of index type.
     */
    public static class Floats extends DenseVectorQuery {

        private final float[] query;

        public Floats(float[] query, String field) {
            super(field);
            this.query = query;
        }

        public float[] getQuery() {
            return query;
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
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats floats = (Floats) o;
            return Objects.equals(field, floats.field) && Objects.deepEquals(query, floats.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query));
        }
    }

    /**
     * Scores using {@code function.compare(target, raw)} against the raw float vectors retrieved via
     * {@link FloatVectorValues#vectorValue(int)}. This produces full-precision scores even on
     * codec-quantized fields, at the cost of bypassing the optimized quantized scorer.
     */
    public static class RawFloats extends DenseVectorQuery {

        private final float[] query;
        private final VectorSimilarityFunction function;

        public RawFloats(float[] query, String field, VectorSimilarityFunction function) {
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
            return "DenseVectorQuery.RawFloats";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new DenseVectorWeight(RawFloats.this, boost) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return new RawFloatVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RawFloats other = (RawFloats) o;
            return Objects.equals(field, other.field) && Objects.deepEquals(query, other.query) && function == other.function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function);
        }
    }

    public static class Bytes extends DenseVectorQuery {

        private final byte[] query;

        public Bytes(byte[] query, String field) {
            super(field);
            this.query = query;
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
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes bytes = (Bytes) o;
            return Objects.equals(field, bytes.field) && Objects.deepEquals(query, bytes.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query));
        }
    }

    /**
     * Scores using {@code function.compare(target, raw)} against raw byte vectors retrieved via
     * {@link ByteVectorValues#vectorValue(int)}. Mirrors {@link RawFloats} for byte/bit element types.
     */
    public static class RawBytes extends DenseVectorQuery {

        private final byte[] query;
        private final VectorSimilarityFunction function;

        public RawBytes(byte[] query, String field, VectorSimilarityFunction function) {
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
            return "DenseVectorQuery.RawBytes";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new DenseVectorWeight(RawBytes.this, boost) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    ByteVectorValues vectorValues = leafReaderContext.reader().getByteVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return new RawByteVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RawBytes other = (RawBytes) o;
            return Objects.equals(field, other.field) && Objects.deepEquals(query, other.query) && function == other.function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function);
        }
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
