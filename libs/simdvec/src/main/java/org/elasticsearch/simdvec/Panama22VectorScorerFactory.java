/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.simdvec.internal.IndexInputUtils;
import org.elasticsearch.simdvec.internal.MemorySegmentES92Int7VectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.MemorySegmentES91OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.MemorySegmentES940OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.OnHeapES91OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.PanamaVectorConstants;

import java.io.IOException;
import java.util.Optional;

final class Panama22VectorScorerFactory implements VectorScorerFactory {

    private static final VectorScorerFactory FALLBACK = new DefaultVectorScorerFactory();

    @Override
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        if (PanamaVectorConstants.ENABLE_INTEGER_VECTORS) {
            IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
            unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
            if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
                return new MemorySegmentES91OSQVectorsScorer(unwrappedInput, dimension, bulkSize);
            }
        }
        return new OnHeapES91OSQVectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES940OSQVectorsScorer newES940OSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding
    ) throws IOException {
        if (PanamaVectorConstants.ENABLE_INTEGER_VECTORS
            && ((queryBits == 4 && (indexBits == 1 || indexBits == 2 || indexBits == 4)) || (queryBits == 7 && indexBits == 7))) {
            IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
            unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
            if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
                return new MemorySegmentES940OSQVectorsScorer(
                    unwrappedInput,
                    queryBits,
                    indexBits,
                    dimension,
                    dataLength,
                    bulkSize,
                    int4Encoding,
                    false   // native not enabled
                );
            }
        }
        return FALLBACK.newES940OSQVectorsScorer(input, queryBits, indexBits, dimension, dataLength, bulkSize, int4Encoding);
    }

    @Override
    public ES92Int7VectorsScorer newES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);

        if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
            // native requires heap segments & native, so this will use panama
            return new MemorySegmentES92Int7VectorsScorer(unwrappedInput, dimension, bulkSize);
        }
        return FALLBACK.newES92Int7VectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES93BinaryQuantizedVectorScorer newES93BinaryQuantizedVectorScorer(IndexInput input, int dimension, int vectorLengthInBytes)
        throws IOException {
        return FALLBACK.newES93BinaryQuantizedVectorScorer(input, dimension, vectorLengthInBytes);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getFloat32VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getBFloat16VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt8VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        ByteVectorValues values
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getFloat32VectorScorer(
        VectorSimilarityFunction sim,
        FloatVectorValues values,
        float[] queryVector
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getBFloat16VectorScorer(
        VectorSimilarityFunction sim,
        FloatVectorValues values,
        float[] queryVector
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getInt8VectorScorer(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getInt7SQVectorScorer(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        float[] queryVector
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7uOSQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getInt7uOSQVectorScorer(
        VectorSimilarityFunction sim,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        byte[] quantizedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt4VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getInt4VectorScorer(
        VectorSimilarityFunction sim,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "Panama22VectorScorerFactory";
    }
}
