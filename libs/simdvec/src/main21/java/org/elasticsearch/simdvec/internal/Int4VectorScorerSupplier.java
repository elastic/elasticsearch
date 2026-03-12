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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4BulkWithOffsets;

/**
 * Int4 packed-nibble scorer supplier.
 * Each stored vector is {@code dims/2} packed bytes (two 4-bit values per byte), followed by
 * corrective terms (3 floats + 1 int). The query is unpacked to {@code dims} bytes before scoring.
 */
public final class Int4VectorScorerSupplier implements RandomVectorScorerSupplier {

    private static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    private final IndexInput input;
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityType similarityType;
    private final int dims;
    private final int packedDims;
    private final int maxOrd;
    private final long vectorPitch;
    private final Int4Corrections.SingleCorrection correction;
    private final Int4Corrections.BulkCorrection bulkCorrection;

    private byte[] scratch;

    public Int4VectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values, VectorSimilarityType similarityType) {
        IndexInputUtils.checkInputType(input);
        this.input = input;
        this.values = values;
        this.similarityType = similarityType;
        this.dims = values.dimension();
        this.packedDims = dims / 2;
        this.maxOrd = values.size();
        this.vectorPitch = packedDims + 3L * Float.BYTES + Integer.BYTES;
        this.correction = Int4Corrections.singleCorrectionFor(similarityType);
        this.bulkCorrection = Int4Corrections.bulkCorrectionFor(similarityType);
    }

    private record QueryContext(
        int ord,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum,
        MemorySegment unpackedQuery
    ) {}

    private QueryContext createQueryContext(int ord) throws IOException {
        var correctiveTerms = values.getCorrectiveTerms(ord);
        long offset = (long) ord * vectorPitch;
        input.seek(offset);
        byte[] packed = new byte[packedDims];
        input.readBytes(packed, 0, packedDims);
        byte[] unpacked = unpackNibbles(packed);
        return new QueryContext(
            ord,
            correctiveTerms.lowerInterval(),
            correctiveTerms.upperInterval(),
            correctiveTerms.additionalCorrection(),
            correctiveTerms.quantizedComponentSum(),
            MemorySegment.ofArray(unpacked)
        );
    }

    private void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
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
            query.lowerInterval,
            query.upperInterval,
            query.additionalCorrection,
            query.quantizedComponentSum
        );
    }

    private float applyCorrectionsBulk(MemorySegment scores, MemorySegment ordinals, int numNodes, QueryContext query) throws IOException {
        return bulkCorrection.apply(
            values,
            dims,
            scores,
            ordinals,
            numNodes,
            query.lowerInterval,
            query.upperInterval,
            query.additionalCorrection,
            query.quantizedComponentSum
        );
    }

    private float scoreFromOrds(QueryContext query, int secondOrd) throws IOException {
        checkOrdinal(query.ord);
        checkOrdinal(secondOrd);
        long secondVectorOffset = secondOrd * vectorPitch;
        input.seek(secondVectorOffset);
        return IndexInputUtils.withSlice(input, packedDims, this::getScratch, packedTarget -> {
            int rawScore = dotProductI4(query.unpackedQuery, packedTarget, packedDims);
            return applyCorrections(rawScore, secondOrd, query);
        });
    }

    private float bulkScoreFromOrds(QueryContext query, int[] ordinals, float[] scores, int numNodes) throws IOException {
        checkOrdinal(query.ord);
        input.seek(0);
        return IndexInputUtils.withSlice(input, input.length(), this::getScratch, vectors -> {
            if (SUPPORTS_HEAP_SEGMENTS) {
                var ordinalsSeg = MemorySegment.ofArray(ordinals);
                var scoresSeg = MemorySegment.ofArray(scores);
                dotProductI4BulkWithOffsets(vectors, query.unpackedQuery, packedDims, (int) vectorPitch, ordinalsSeg, numNodes, scoresSeg);
                return applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes, query);
            } else {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment ordinalsSeg = arena.allocate((long) numNodes * Integer.BYTES, Integer.BYTES);
                    MemorySegment scoresSeg = arena.allocate((long) numNodes * Float.BYTES, Float.BYTES);
                    MemorySegment.copy(ordinals, 0, ordinalsSeg, ValueLayout.JAVA_INT, 0, numNodes);
                    dotProductI4BulkWithOffsets(
                        vectors,
                        query.unpackedQuery,
                        packedDims,
                        (int) vectorPitch,
                        ordinalsSeg,
                        numNodes,
                        scoresSeg
                    );
                    float max = applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes, query);
                    MemorySegment.copy(scoresSeg, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                    return max;
                }
            }
        });
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
        return new Int4VectorScorerSupplier(input.clone(), values.copy(), similarityType);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private QueryContext query;

            @Override
            public float score(int node) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return scoreFromOrds(query, node);
            }

            @Override
            public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return bulkScoreFromOrds(query, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                query = createQueryContext(node);
            }
        };
    }

    public QuantizedByteVectorValues get() {
        return values;
    }

    static byte[] unpackNibbles(byte[] packed) {
        int packedLen = packed.length;
        byte[] unpacked = new byte[packedLen * 2];
        for (int i = 0; i < packedLen; i++) {
            unpacked[i] = (byte) ((packed[i] & 0xFF) >>> 4);
            unpacked[i + packedLen] = (byte) (packed[i] & 0x0F);
        }
        return unpacked;
    }
}
