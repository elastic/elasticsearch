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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.internal.BFloat16VectorScorer;
import org.elasticsearch.simdvec.internal.BFloat16VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.ByteVectorScorer;
import org.elasticsearch.simdvec.internal.ByteVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.FloatVectorScorer;
import org.elasticsearch.simdvec.internal.FloatVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int4VectorScorer;
import org.elasticsearch.simdvec.internal.Int4VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7uOSQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7uOSQVectorScorerSupplier;

import java.util.Optional;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

final class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE;

    private VectorScorerFactoryImpl() {}

    static {
        INSTANCE = NativeAccess.instance().getVectorSimilarityFunctions().map(ignore -> new VectorScorerFactoryImpl()).orElse(null);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getFloatVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new FloatVectorScorerSupplier.DotProductSupplier(input, values));
            case EUCLIDEAN -> Optional.of(new FloatVectorScorerSupplier.EuclideanSupplier(input, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new FloatVectorScorerSupplier.MaxInnerProductSupplier(input, values));
        };
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getBFloat16VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
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
    public Optional<RandomVectorScorerSupplier> getByteVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        ByteVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.dimension(), input);
            return switch (similarityType) {
                case COSINE -> Optional.of(new ByteVectorScorerSupplier.CosineSupplier(msInput, values));
                case DOT_PRODUCT -> Optional.of(new ByteVectorScorerSupplier.DotProductSupplier(msInput, values));
                case EUCLIDEAN -> Optional.of(new ByteVectorScorerSupplier.EuclideanSupplier(msInput, values));
                case MAXIMUM_INNER_PRODUCT -> Optional.of(new ByteVectorScorerSupplier.MaxInnerProductSupplier(msInput, values));
            };
        }
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getFloatVectorScorer(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        return FloatVectorScorer.create(sim, values, queryVector);
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
    public Optional<RandomVectorScorer> getByteVectorScorer(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        return ByteVectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
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
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
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
}
