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
import org.elasticsearch.simdvec.internal.BFloat16VectorScorer;
import org.elasticsearch.simdvec.internal.BFloat16VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Float32VectorScorer;
import org.elasticsearch.simdvec.internal.Float32VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.IndexInputUtils;
import org.elasticsearch.simdvec.internal.Int4VectorScorer;
import org.elasticsearch.simdvec.internal.Int4VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7uOSQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7uOSQVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int8VectorScorer;
import org.elasticsearch.simdvec.internal.Int8VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.MemorySegmentES92Int7VectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.MemorySegmentES940OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.NativeBinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.internal.vectorization.PanamaVectorConstants;

import java.io.IOException;
import java.util.Optional;

final class Native22VectorScorerFactory implements VectorScorerFactory {

    private static final VectorScorerFactory FALLBACK = new Panama22VectorScorerFactory();

    @Override
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        return FALLBACK.newES91OSQVectorsScorer(input, dimension, bulkSize);
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
                    true
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
            return new MemorySegmentES92Int7VectorsScorer(unwrappedInput, dimension, bulkSize);
        }
        return FALLBACK.newES92Int7VectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES93BinaryQuantizedVectorScorer newES93BinaryQuantizedVectorScorer(IndexInput input, int dimensions, int vectorLengthInBytes)
        throws IOException {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
        return new NativeBinaryQuantizedVectorScorer(unwrappedInput, dimensions, vectorLengthInBytes);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getFloat32VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new Float32VectorScorerSupplier.DotProductSupplier(input, values));
            case EUCLIDEAN -> Optional.of(new Float32VectorScorerSupplier.EuclideanSupplier(input, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new Float32VectorScorerSupplier.MaxInnerProductSupplier(input, values));
        };
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getBFloat16VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.getVectorByteLength(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new BFloat16VectorScorerSupplier.DotProductSupplier(input, values));
            case EUCLIDEAN -> Optional.of(new BFloat16VectorScorerSupplier.EuclideanSupplier(input, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new BFloat16VectorScorerSupplier.MaxInnerProductSupplier(input, values));
        };
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt8VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        ByteVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE -> Optional.of(new Int8VectorScorerSupplier.CosineSupplier(input, values));
            case DOT_PRODUCT -> Optional.of(new Int8VectorScorerSupplier.DotProductSupplier(input, values));
            case EUCLIDEAN -> Optional.of(new Int8VectorScorerSupplier.EuclideanSupplier(input, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new Int8VectorScorerSupplier.MaxInnerProductSupplier(input, values));
        };
    }

    @Override
    public Optional<RandomVectorScorer> getFloat32VectorScorer(
        VectorSimilarityFunction sim,
        FloatVectorValues values,
        float[] queryVector
    ) {
        return Float32VectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorer> getBFloat16VectorScorer(
        VectorSimilarityFunction sim,
        FloatVectorValues values,
        float[] queryVector
    ) {
        return BFloat16VectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorer> getInt8VectorScorer(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        return Int8VectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(
                new Int7SQVectorScorerSupplier.DotProductSupplier(input, values, scoreCorrectionConstant)
            );
            case EUCLIDEAN -> Optional.of(new Int7SQVectorScorerSupplier.EuclideanSupplier(input, values, scoreCorrectionConstant));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(
                new Int7SQVectorScorerSupplier.MaxInnerProductSupplier(input, values, scoreCorrectionConstant)
            );
        };
    }

    @Override
    public Optional<RandomVectorScorer> getInt7SQVectorScorer(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        float[] queryVector
    ) {
        return Int7SQVectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7uOSQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new Int7uOSQVectorScorerSupplier.DotProductSupplier(input, values));
            case EUCLIDEAN -> Optional.of(new Int7uOSQVectorScorerSupplier.EuclideanSupplier(input, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new Int7uOSQVectorScorerSupplier.MaxInnerProductSupplier(input, values));
        };
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
        return Int7uOSQVectorScorer.create(
            sim,
            values,
            quantizedQuery,
            lowerInterval,
            upperInterval,
            additionalCorrection,
            quantizedComponentSum
        );
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt4VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension() / 2, input);
        return Optional.of(new Int4VectorScorerSupplier(input, values, similarityType));
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
        return Int4VectorScorer.create(
            sim,
            values,
            unpackedQuery,
            lowerInterval,
            upperInterval,
            additionalCorrection,
            quantizedComponentSum
        );
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }

    @Override
    public String toString() {
        return "Native22VectorScorerFactory";
    }
}
