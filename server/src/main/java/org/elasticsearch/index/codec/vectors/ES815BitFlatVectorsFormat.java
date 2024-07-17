/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.elasticsearch.script.field.vectors.ESVectorUtil;

import java.io.IOException;

class ES815BitFlatVectorsFormat extends FlatVectorsFormat {

    private final FlatVectorsFormat delegate = new Lucene99FlatVectorsFormat(FlatBitVectorScorer.INSTANCE);

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState segmentWriteState) throws IOException {
        return delegate.fieldsWriter(segmentWriteState);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState segmentReadState) throws IOException {
        return delegate.fieldsReader(segmentReadState);
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
            RandomAccessVectorValues randomAccessVectorValues
        ) throws IOException {
            assert randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes;
            assert vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;
            if (randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes randomAccessVectorValuesBytes) {
                assert randomAccessVectorValues instanceof RandomAccessQuantizedByteVectorValues == false;
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE, EUCLIDEAN -> new HammingScorerSupplier(randomAccessVectorValuesBytes);
                };
            }
            throw new IllegalArgumentException("Unsupported vector type or similarity function");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction vectorSimilarityFunction,
            RandomAccessVectorValues randomAccessVectorValues,
            byte[] bytes
        ) {
            assert randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes;
            assert vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;
            if (randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes randomAccessVectorValuesBytes) {
                checkDimensions(bytes.length, randomAccessVectorValuesBytes.dimension());
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE, EUCLIDEAN -> new HammingVectorScorer(
                        randomAccessVectorValuesBytes,
                        bytes
                    );
                };
            }
            throw new IllegalArgumentException("Unsupported vector type or similarity function");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction vectorSimilarityFunction,
            RandomAccessVectorValues randomAccessVectorValues,
            float[] floats
        ) {
            throw new IllegalArgumentException("Unsupported vector type");
        }
    }

    static float hammingScore(byte[] a, byte[] b) {
        return ((a.length * Byte.SIZE) - ESVectorUtil.xorBitCount(a, b)) / (float) (a.length * Byte.SIZE);
    }

    static class HammingVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final byte[] query;
        private final RandomAccessVectorValues.Bytes byteValues;

        HammingVectorScorer(RandomAccessVectorValues.Bytes byteValues, byte[] query) {
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
        private final RandomAccessVectorValues.Bytes byteValues, byteValues1, byteValues2;

        HammingScorerSupplier(RandomAccessVectorValues.Bytes byteValues) throws IOException {
            this.byteValues = byteValues;
            this.byteValues1 = byteValues.copy();
            this.byteValues2 = byteValues.copy();
        }

        @Override
        public RandomVectorScorer scorer(int i) throws IOException {
            byte[] query = byteValues1.vectorValue(i);
            return new HammingVectorScorer(byteValues2, query);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new HammingScorerSupplier(byteValues);
        }
    }

}
