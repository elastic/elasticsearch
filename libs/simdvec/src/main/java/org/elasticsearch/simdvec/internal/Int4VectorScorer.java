/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4BulkSparse;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

/**
 * Int4 packed-nibble query-time scorer. The float query is quantized externally
 * and passed in as unpacked bytes (one byte per dimension, 0-15 range) along
 * with corrective terms. Each stored vector is {@code dims/2} packed bytes
 * followed by corrective terms (3 floats + 1 int).
 */
public final class Int4VectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    private final ScorerImpl scorerImpl;
    private final QueryContext query;

    /**
     * Creates an int4 query-time scorer if the input supports efficient access.
     *
     * @param sim                   the similarity function
     * @param values                the quantized vector values
     * @param unpackedQuery         the quantized query (dims bytes, one per dimension, 0-15)
     * @param lowerInterval         query corrective term
     * @param upperInterval         query corrective term
     * @param additionalCorrection  query corrective term
     * @param quantizedComponentSum query corrective term
     * @return an optional scorer or empty if the input doesn't support native access
     */
    public static Optional<RandomVectorScorer> create(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        IndexInput input = values.getSlice();
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        return Optional.of(
            new Int4VectorScorer(
                input,
                values,
                VectorSimilarityType.of(sim),
                unpackedQuery,
                lowerInterval,
                upperInterval,
                additionalCorrection,
                quantizedComponentSum
            )
        );
    }

    Int4VectorScorer(
        IndexInput input,
        QuantizedByteVectorValues values,
        VectorSimilarityType similarityType,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        super(values);
        IndexInputUtils.checkInputType(input);
        int dims = values.dimension();
        int packedDims = dims / 2;
        long vectorPitch = packedDims + 3L * Float.BYTES + Integer.BYTES;

        this.scorerImpl = new ScorerImpl(
            input,
            values,
            dims,
            packedDims,
            vectorPitch,
            Int4Corrections.singleCorrectionFor(similarityType),
            Int4Corrections.bulkCorrectionFor(similarityType)
        );

        final MemorySegment unpackedQuerySegment;
        if (SUPPORTS_HEAP_SEGMENTS) {
            unpackedQuerySegment = MemorySegment.ofArray(unpackedQuery);
        } else {
            unpackedQuerySegment = Arena.ofAuto().allocate(unpackedQuery.length, 32);
            MemorySegment.copy(unpackedQuery, 0, unpackedQuerySegment, ValueLayout.JAVA_BYTE, 0, unpackedQuery.length);
        }

        this.query = new QueryContext(lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum, unpackedQuerySegment);
    }

    @Override
    public float score(int node) throws IOException {
        return scorerImpl.scoreWithQuery(query, node);
    }

    @Override
    public float bulkScore(int[] ordinals, float[] scores, int numNodes) throws IOException {
        return scorerImpl.bulkScoreWithQuery(query, ordinals, scores, numNodes);
    }

    /**
     * Shared scoring implementation used by both {@link Int4VectorScorer} (query-time) and
     * {@link Int4VectorScorerSupplier} (graph-build / reranking).
     * Not thread-safe under all conditions (due to mutable state (scratch) used by IndexInput):
     * each supplier/scorer should own its own instance.
     */
    static class ScorerImpl {
        private final IndexInput input;
        private final QuantizedByteVectorValues values;
        private final int dims;
        private final int packedDims;
        private final long vectorPitch;
        private final Int4Corrections.SingleCorrection correction;
        private final Int4Corrections.BulkCorrection bulkCorrection;
        private byte[] scratch;

        ScorerImpl(
            IndexInput input,
            QuantizedByteVectorValues values,
            int dims,
            int packedDims,
            long vectorPitch,
            Int4Corrections.SingleCorrection correction,
            Int4Corrections.BulkCorrection bulkCorrection
        ) {
            this.input = input;
            this.values = values;
            this.dims = dims;
            this.packedDims = packedDims;
            this.vectorPitch = vectorPitch;
            this.correction = correction;
            this.bulkCorrection = bulkCorrection;
        }

        void checkOrdinal(int ord) {
            if (ord < 0 || ord >= values.size()) {
                throw new IllegalArgumentException("illegal ordinal: " + ord);
            }
        }

        private byte[] getScratch(int len) {
            if (scratch == null || scratch.length < len) {
                scratch = new byte[len];
            }
            return scratch;
        }

        private float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException {
            return correction.apply(
                values,
                dims,
                rawScore,
                ord,
                query.lowerInterval(),
                query.upperInterval(),
                query.additionalCorrection(),
                query.quantizedComponentSum()
            );
        }

        private float applyCorrectionsBulk(MemorySegment scores, MemorySegment ordinals, int numNodes, QueryContext query)
            throws IOException {
            return bulkCorrection.apply(
                values,
                dims,
                scores,
                ordinals,
                numNodes,
                query.lowerInterval(),
                query.upperInterval(),
                query.additionalCorrection(),
                query.quantizedComponentSum()
            );
        }

        float scoreWithQuery(QueryContext query, int node) throws IOException {
            checkOrdinal(node);
            long nodeOffset = (long) node * vectorPitch;
            input.seek(nodeOffset);
            return IndexInputUtils.withSlice(input, packedDims, this::getScratch, packedTarget -> {
                int rawScore = dotProductI4(query.unpackedQuery(), packedTarget, packedDims);
                return applyCorrections(rawScore, node, query);
            });
        }

        float bulkScoreWithQuery(QueryContext query, int[] ordinals, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }
            if (SUPPORTS_HEAP_SEGMENTS) {
                long[] offsets = new long[numNodes];
                for (int i = 0; i < numNodes; i++) {
                    offsets[i] = (long) ordinals[i] * vectorPitch;
                }
                boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, packedDims, numNodes, addrs -> {
                    dotProductI4BulkSparse(addrs, query.unpackedQuery(), packedDims, numNodes, MemorySegment.ofArray(scores));
                });
                if (resolved) {
                    return applyCorrectionsBulk(MemorySegment.ofArray(scores), MemorySegment.ofArray(ordinals), numNodes, query);
                }
            }

            // fallback to sequential scoring
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                scores[i] = scoreWithQuery(query, ordinals[i]);
                max = Math.max(max, scores[i]);
            }
            return max;
        }
    }

    record QueryContext(
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum,
        MemorySegment unpackedQuery
    ) {}
}
