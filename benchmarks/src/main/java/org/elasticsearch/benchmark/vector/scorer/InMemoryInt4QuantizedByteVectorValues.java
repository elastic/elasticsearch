/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import java.io.IOException;

/**
 * In-memory implementation of {@link QuantizedByteVectorValues} for int4 (PACKED_NIBBLE) benchmarks.
 * Stores pre-quantized packed nibble vectors with synthetic corrective terms.
 */
class InMemoryInt4QuantizedByteVectorValues extends QuantizedByteVectorValues {

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
    public ScalarEncoding getScalarEncoding() {
        return ScalarEncoding.PACKED_NIBBLE;
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
