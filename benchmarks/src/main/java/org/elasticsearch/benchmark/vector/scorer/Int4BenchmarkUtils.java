/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.applyI4Corrections;
import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.dotProductI4SinglePacked;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.unpackNibbles;

public class Int4BenchmarkUtils {

    /**
     * In-memory implementation of {@link QuantizedByteVectorValues} for int4 (PACKED_NIBBLE) benchmarks.
     * Stores pre-quantized packed nibble vectors with synthetic corrective terms.
     */
    static class InMemoryInt4QuantizedByteVectorValues extends QuantizedByteVectorValues {

        private final int dims;
        private final byte[][] packedVectors;
        private final OptimizedScalarQuantizer.QuantizationResult[] correctiveTerms;
        private final float[] centroid;
        private final float centroidDP;
        private final OptimizedScalarQuantizer quantizer;

        InMemoryInt4QuantizedByteVectorValues(
            int dims,
            byte[][] packedVectors,
            OptimizedScalarQuantizer.QuantizationResult[] correctiveTerms,
            float[] centroid,
            float centroidDP
        ) {
            this.dims = dims;
            this.packedVectors = packedVectors;
            this.correctiveTerms = correctiveTerms;
            this.centroid = centroid;
            this.centroidDP = centroidDP;
            this.quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);
        }

        @Override
        public int dimension() {
            return dims;
        }

        @Override
        public int size() {
            return packedVectors.length;
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            return packedVectors[ord];
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd) throws IOException {
            return correctiveTerms[vectorOrd];
        }

        @Override
        public OptimizedScalarQuantizer getQuantizer() {
            return quantizer;
        }

        @Override
        public Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding getScalarEncoding() {
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
        }

        @Override
        public float[] getCentroid() throws IOException {
            return centroid;
        }

        @Override
        public float getCentroidDP() throws IOException {
            return centroidDP;
        }

        @Override
        public VectorScorer scorer(float[] query) throws IOException {
            return null;
        }

        @Override
        public InMemoryInt4QuantizedByteVectorValues copy() throws IOException {
            return new InMemoryInt4QuantizedByteVectorValues(dims, packedVectors, correctiveTerms, centroid, centroidDP);
        }
    }

    private static class ScalarScorer implements UpdateableRandomVectorScorer {
        private final QuantizedByteVectorValues values;
        private final int dims;
        private final VectorSimilarityFunction similarityFunction;

        private byte[] queryUnpacked;
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections;

        ScalarScorer(QuantizedByteVectorValues values, VectorSimilarityFunction similarityFunction) {
            this.values = values;
            this.dims = values.dimension();
            this.similarityFunction = similarityFunction;
        }

        @Override
        public float score(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            int rawDot = dotProductI4SinglePacked(queryUnpacked, packed);
            var nodeCorrections = values.getCorrectiveTerms(node);
            return applyI4Corrections(rawDot, dims, nodeCorrections, queryCorrections, values.getCentroidDP(), similarityFunction);
        }

        @Override
        public int maxOrd() {
            return values.size();
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            queryUnpacked = unpackNibbles(packed, dims);
            queryCorrections = values.getCorrectiveTerms(node);
        }
    }

    static QuantizedByteVectorValues createI4QuantizedVectorValues(int dims, byte[][] packedVectors) {
        var random = ThreadLocalRandom.current();
        var correctiveTerms = new OptimizedScalarQuantizer.QuantizationResult[packedVectors.length];
        for (int i = 0; i < packedVectors.length; i++) {
            correctiveTerms[i] = new OptimizedScalarQuantizer.QuantizationResult(
                random.nextFloat(-1f, 1f),
                random.nextFloat(-1f, 1f),
                random.nextFloat(-1f, 1f),
                random.nextInt(0, dims * 15)
            );
        }
        float[] centroid = new float[dims];
        for (int i = 0; i < dims; i++) {
            centroid[i] = random.nextFloat();
        }
        float centroidDP = random.nextFloat();
        return new InMemoryInt4QuantizedByteVectorValues(dims, packedVectors, correctiveTerms, centroid, centroidDP);
    }

    static UpdateableRandomVectorScorer createI4ScalarScorer(
        QuantizedByteVectorValues values,
        VectorSimilarityFunction similarityFunction
    ) {
        return new ScalarScorer(values, similarityFunction);
    }

    static RandomVectorScorer createI4ScalarQueryScorer(
        QuantizedByteVectorValues values,
        VectorSimilarityFunction similarityFunction,
        float[] queryVector
    ) throws IOException {
        int dims = values.dimension();
        OptimizedScalarQuantizer quantizer = values.getQuantizer();
        float[] centroid = values.getCentroid();
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding = values.getScalarEncoding();

        byte[] queryQuantized = new byte[encoding.getDiscreteDimensions(dims)];
        float[] queryCopy = Arrays.copyOf(queryVector, queryVector.length);
        if (similarityFunction == VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(queryCopy);
        }
        var queryCorrections = quantizer.scalarQuantize(queryCopy, queryQuantized, encoding.getQueryBits(), centroid);
        float centroidDP = values.getCentroidDP();

        return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
            @Override
            public float score(int node) throws IOException {
                byte[] packed = values.vectorValue(node);
                int rawDot = dotProductI4SinglePacked(queryQuantized, packed);
                var nodeCorrections = values.getCorrectiveTerms(node);
                return applyI4Corrections(rawDot, dims, nodeCorrections, queryCorrections, centroidDP, similarityFunction);
            }
        };
    }
}
