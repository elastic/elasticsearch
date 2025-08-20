/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

class ES815BitFlatVectorsFormat extends FlatVectorsFormat {

    private static final FlatVectorsFormat delegate = new Lucene99FlatVectorsFormat(FlatBitVectorScorer.INSTANCE);

    protected ES815BitFlatVectorsFormat() {
        super("ES815BitFlatVectorsFormat");
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState segmentWriteState) throws IOException {
        return delegate.fieldsWriter(segmentWriteState);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState segmentReadState) throws IOException {
        return delegate.fieldsReader(segmentReadState);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    static class FlatBitVectorScorer implements FlatVectorsScorer {

        static final FlatBitVectorScorer INSTANCE = new FlatBitVectorScorer();

        static void checkDimensions(int queryLen, int fieldLen) {
            if (queryLen != fieldLen) {
                throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
            }
        }

        @Override
        public String toString() {
            return super.toString();
        }

        @Override
        public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
            VectorSimilarityFunction vectorSimilarityFunction,
            KnnVectorValues vectorValues
        ) throws IOException {
            assert vectorValues instanceof ByteVectorValues;
            assert vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;
            if (vectorValues instanceof ByteVectorValues byteVectorValues) {
                assert byteVectorValues instanceof QuantizedByteVectorValues == false;
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE, EUCLIDEAN -> new HammingScorerSupplier(byteVectorValues);
                };
            }
            throw new IllegalArgumentException("Unsupported vector type or similarity function");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction vectorSimilarityFunction,
            KnnVectorValues vectorValues,
            byte[] target
        ) throws IOException {
            assert vectorValues instanceof ByteVectorValues;
            assert vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;
            if (vectorValues instanceof ByteVectorValues byteVectorValues) {
                checkDimensions(target.length, byteVectorValues.dimension());
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE, EUCLIDEAN -> new HammingVectorScorer(byteVectorValues, target);
                };
            }
            throw new IllegalArgumentException("Unsupported vector type or similarity function");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction similarityFunction,
            KnnVectorValues vectorValues,
            float[] target
        ) throws IOException {
            throw new IllegalArgumentException("Unsupported vector type");
        }
    }

    static float hammingScore(byte[] a, byte[] b) {
        return ((a.length * Byte.SIZE) - VectorUtil.xorBitCount(a, b)) / (float) (a.length * Byte.SIZE);
    }

    static class HammingVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final byte[] query;
        private final ByteVectorValues byteValues;

        HammingVectorScorer(ByteVectorValues byteValues, byte[] query) {
            super(byteValues);
            this.query = query;
            this.byteValues = byteValues;
        }

        @Override
        public float score(int i) throws IOException {
            return hammingScore(byteValues.vectorValue(i), query);
        }
    }

    static class HammingScorerSupplier implements RandomVectorScorerSupplier {
        private final ByteVectorValues byteValues, targetValues;

        HammingScorerSupplier(ByteVectorValues byteValues) throws IOException {
            this.byteValues = byteValues;
            this.targetValues = byteValues.copy();
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(targetValues) {
                private final byte[] query = new byte[targetValues.dimension()];
                private int currentOrd = -1;

                @Override
                public void setScoringOrdinal(int i) throws IOException {
                    if (currentOrd == i) {
                        return;
                    }
                    System.arraycopy(targetValues.vectorValue(i), 0, query, 0, query.length);
                    this.currentOrd = i;
                }

                @Override
                public float score(int i) throws IOException {
                    return hammingScore(targetValues.vectorValue(i), query);
                }
            };
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new HammingScorerSupplier(byteValues);
        }
    }

}
