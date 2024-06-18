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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

import java.io.IOException;

import static org.apache.lucene.util.BitUtil.VH_NATIVE_LONG;

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
            if (randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes randomAccessVectorValuesBytes) {
                assert randomAccessVectorValues instanceof RandomAccessQuantizedByteVectorValues == false;
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE -> new AndScorerSupplier(randomAccessVectorValuesBytes);
                    case EUCLIDEAN -> new HammingScorerSupplier(randomAccessVectorValuesBytes);
                };
            }
            throw new IllegalArgumentException("Unsupported vector type");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction vectorSimilarityFunction,
            RandomAccessVectorValues randomAccessVectorValues,
            byte[] bytes
        ) throws IOException {
            if (randomAccessVectorValues instanceof RandomAccessVectorValues.Bytes randomAccessVectorValuesBytes) {
                checkDimensions(bytes.length, randomAccessVectorValuesBytes.dimension());
                return switch (vectorSimilarityFunction) {
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT, COSINE -> new AndVectorScorer(randomAccessVectorValuesBytes, bytes);
                    case EUCLIDEAN -> new HammingVectorScorer(randomAccessVectorValuesBytes, bytes);
                };
            }
            throw new IllegalArgumentException("Unsupported vector type");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(
            VectorSimilarityFunction vectorSimilarityFunction,
            RandomAccessVectorValues randomAccessVectorValues,
            float[] floats
        ) throws IOException {
            throw new IllegalArgumentException("Unsupported vector type");
        }
    }

    static int andBitCount(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
        } else {
            int distance = 0;
            int i = 0;
            for (int upperBound = a.length & -8; i < upperBound; i += 8) {
                distance += Long.bitCount((long) VH_NATIVE_LONG.get(a, i) & (long) VH_NATIVE_LONG.get(b, i));
            }
            while (i < a.length) {
                distance += Integer.bitCount((a[i] & b[i]) & 255);
                ++i;
            }
            return distance;
        }
    }

    static float hammingScore(byte[] a, byte[] b) {
        return ((a.length * Byte.SIZE) - VectorUtil.xorBitCount(a, b)) / (float) (a.length * Byte.SIZE);
    }

    static float andScore(byte[] a, byte[] b) {
        return andBitCount(a, b) / (float) (a.length * Byte.SIZE);
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

    static class AndVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final byte[] query;
        private final RandomAccessVectorValues.Bytes byteValues;

        AndVectorScorer(RandomAccessVectorValues.Bytes byteValues, byte[] query) {
            super(byteValues);
            this.query = query;
            this.byteValues = byteValues;
        }

        @Override
        public float score(int i) throws IOException {
            return andScore(byteValues.vectorValue(i), query);
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

    static class AndScorerSupplier implements RandomVectorScorerSupplier {
        private final RandomAccessVectorValues.Bytes byteValues, byteValues1, byteValues2;

        AndScorerSupplier(RandomAccessVectorValues.Bytes byteValues) throws IOException {
            this.byteValues = byteValues;
            this.byteValues1 = byteValues.copy();
            this.byteValues2 = byteValues.copy();
        }

        @Override
        public RandomVectorScorer scorer(int i) throws IOException {
            byte[] query = byteValues1.vectorValue(i);
            return new AndVectorScorer(byteValues2, query);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new AndScorerSupplier(byteValues);
        }

    }

}
