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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.applyI4Corrections;
import static org.elasticsearch.nativeaccess.Int4TestUtils.dotProductI4SinglePacked;
import static org.elasticsearch.nativeaccess.Int4TestUtils.unpackNibbles;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writePackedVectorWithCorrection;

public class Int4BenchmarkUtils {

    static final String VECTOR_DATA_FILE = "int4-vector.data";

    static void writeI4VectorData(Directory dir, byte[][] packedVectors, OptimizedScalarQuantizer.QuantizationResult[] corrections)
        throws IOException {
        try (IndexOutput out = dir.createOutput(VECTOR_DATA_FILE, IOContext.DEFAULT)) {
            for (int i = 0; i < packedVectors.length; i++) {
                writePackedVectorWithCorrection(out, packedVectors[i], corrections[i]);
            }
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
        float[] queryCopy = queryVector.clone();
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

    static OptimizedScalarQuantizer.QuantizationResult[] generateCorrectiveTerms(int dims, int numVectors) {
        var random = ThreadLocalRandom.current();
        var correctiveTerms = new OptimizedScalarQuantizer.QuantizationResult[numVectors];
        for (int i = 0; i < numVectors; i++) {
            correctiveTerms[i] = new OptimizedScalarQuantizer.QuantizationResult(
                random.nextFloat(-1f, 1f),
                random.nextFloat(-1f, 1f),
                random.nextFloat(-1f, 1f),
                random.nextInt(0, dims * 15)
            );
        }
        return correctiveTerms;
    }

    static float[] generateCentroid(int dims) {
        var random = ThreadLocalRandom.current();
        float[] centroid = new float[dims];
        for (int i = 0; i < dims; i++) {
            centroid[i] = random.nextFloat();
        }
        return centroid;
    }

    /**
     * Quantizes a float query vector for use with the native Int4 scorer.
     * Returns the unpacked quantized bytes (one byte per dimension, 0-15 range).
     */
    static QuantizedQuery quantizeQuery(QuantizedByteVectorValues values, VectorSimilarityFunction sim, float[] queryVector)
        throws IOException {
        int dims = values.dimension();
        OptimizedScalarQuantizer quantizer = values.getQuantizer();
        float[] centroid = values.getCentroid();
        var encoding = values.getScalarEncoding();

        byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];
        float[] queryCopy = queryVector.clone();
        if (sim == VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(queryCopy);
        }
        var corrections = quantizer.scalarQuantize(queryCopy, scratch, encoding.getQueryBits(), centroid);
        byte[] unpackedQuery = Arrays.copyOf(scratch, dims);
        return new QuantizedQuery(unpackedQuery, corrections);
    }

    record QuantizedQuery(byte[] unpackedQuery, OptimizedScalarQuantizer.QuantizationResult corrections) {}
}
