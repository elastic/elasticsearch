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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

import java.io.IOException;
import java.util.Optional;

public interface VectorScorerFactory {

    /** Create a new {@link ES91OSQVectorsScorer} for the given {@link IndexInput}. */
    ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException;

    /**
     * Create a new {@link ES940OSQVectorsScorer} for the given {@link IndexInput} and explicit int4 disk format.
     * The input should be unwrapped before calling this method. If the input is
     * still a {@code FilterIndexInput} that does not implement
     * {@code MemorySegmentAccessInput} or {@code DirectAccessInput}, an
     * {@link IllegalArgumentException} is thrown. Non-wrapper inputs (e.g.
     * {@code ByteBuffersIndexInput}) are accepted and use a heap-copy fallback.
     */
    ES940OSQVectorsScorer newES940OSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding
    ) throws IOException;

    /**
     * Create a new {@link ES92Int7VectorsScorer} for the given {@link IndexInput}.
     * See {@link #newES940OSQVectorsScorer} for input type requirements.
     */
    ES92Int7VectorsScorer newES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException;

    ES93BinaryQuantizedVectorScorer newES93BinaryQuantizedVectorScorer(IndexInput input, int dimensions, int vectorLengthInBytes)
        throws IOException;

    /**
     * Returns an optional containing a float vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd + Float#BYTES) * dims
     * @param values the random access vector values
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getFloat32VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    );

    /**
     * Returns an optional containing a bfloat16 vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd) * dims * 2
     * @param values the random access vector values
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getBFloat16VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    );

    /**
     * Returns an optional containing a byte vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd) * dims
     * @param values the random access vector values
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getInt8VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        ByteVectorValues values
    );

    /**
     * Returns an optional containing a float vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @param queryVector the query vector
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getFloat32VectorScorer(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector);

    /**
     * Returns an optional containing a bfloat16 vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @param queryVector the query vector
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getBFloat16VectorScorer(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector);

    /**
     * Returns an optional containing a byte vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @param queryVector the query vector
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getInt8VectorScorer(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector);

    /**
     * Returns an optional containing an int7 scalar quantized vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd + Float#BYTES) * dims
     * @param values the random access vector values
     * @param scoreCorrectionConstant the score correction constant
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    );

    /**
     * Returns an optional containing an int7 scalar quantized vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @param queryVector the query vector
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getInt7SQVectorScorer(VectorSimilarityFunction sim, QuantizedByteVectorValues values, float[] queryVector);

    /**
     * Returns an optional containing an int7 optimal scalar quantized vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input          the index input containing the vector data
     * @param values         the random access vector values
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getInt7uOSQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    );

    /**
     * Returns an optional containing an int7 optimal scalar quantized vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getInt7uOSQVectorScorer(
        VectorSimilarityFunction sim,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        byte[] quantizedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    );

    /**
     * Returns an optional containing an int4 packed-nibble vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input          the index input containing the vector data
     * @param values         the random access vector values
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getInt4VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values
    );

    /**
     * Returns an optional containing an int4 packed-nibble query-time vector scorer
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim                    the similarity function
     * @param values                 the quantized vector values
     * @param unpackedQuery          the quantized query bytes (one byte per dimension, 0-15)
     * @param lowerInterval          query corrective term
     * @param upperInterval          query corrective term
     * @param additionalCorrection   query corrective term
     * @param quantizedComponentSum  query corrective term
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getInt4VectorScorer(
        VectorSimilarityFunction sim,
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    );
}
