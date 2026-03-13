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

/**
 * Int4 packed-nibble scorer supplier.
 * Each stored vector is {@code dims/2} packed bytes (two 4-bit values per byte), followed by
 * corrective terms (3 floats + 1 int). The query is unpacked to {@code dims} bytes before scoring.
 */
public final class Int4VectorScorerSupplier implements RandomVectorScorerSupplier {

    private final IndexInput input;
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityType similarityType;
    private final int packedDims;
    private final long vectorPitch;
    private final Int4VectorScorer.ScorerImpl scorerImpl;
    private final MemorySegment unpackedQuerySegment;

    public Int4VectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values, VectorSimilarityType similarityType) {
        IndexInputUtils.checkInputType(input);
        int dims = values.dimension();

        this.input = input;
        this.values = values;
        this.similarityType = similarityType;
        this.packedDims = dims / 2;
        this.vectorPitch = packedDims + 3L * Float.BYTES + Integer.BYTES;
        this.unpackedQuerySegment = Arena.ofAuto().allocate(dims, 32);
        this.scorerImpl = new Int4VectorScorer.ScorerImpl(
            input,
            values,
            dims,
            packedDims,
            vectorPitch,
            Int4Corrections.singleCorrectionFor(similarityType),
            Int4Corrections.bulkCorrectionFor(similarityType)
        );
    }

    private Int4VectorScorer.QueryContext createQueryContext(int ord) throws IOException {
        var correctiveTerms = values.getCorrectiveTerms(ord);
        long offset = (long) ord * vectorPitch;
        input.seek(offset);
        byte[] packed = new byte[packedDims];
        input.readBytes(packed, 0, packedDims);
        unpackNibbles(packed);
        return new Int4VectorScorer.QueryContext(
            correctiveTerms.lowerInterval(),
            correctiveTerms.upperInterval(),
            correctiveTerms.additionalCorrection(),
            correctiveTerms.quantizedComponentSum(),
            unpackedQuerySegment
        );
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
        return new Int4VectorScorerSupplier(input.clone(), values.copy(), similarityType);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            /** QueryContext instances used by this scorer are all backed by the same pre-allocated segment
             * (see {@link Int4VectorScorerSupplier#createQueryContext}).
             * The segment is reused across setScoringOrdinal calls; only the most recent one is valid.
             * This makes this scorer and supplier not thread-safe.
             */
            private Int4VectorScorer.QueryContext query;

            @Override
            public float score(int node) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return scorerImpl.scoreWithQuery(query, node);
            }

            @Override
            public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return scorerImpl.bulkScoreWithQuery(query, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                scorerImpl.checkOrdinal(node);
                query = createQueryContext(node);
            }
        };
    }

    public QuantizedByteVectorValues get() {
        return values;
    }

    private void unpackNibbles(byte[] packed) {
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            unpackedQuerySegment.setAtIndex(ValueLayout.JAVA_BYTE, i, (byte) ((packed[i] & 0xFF) >>> 4));
            unpackedQuerySegment.setAtIndex(ValueLayout.JAVA_BYTE, i + packedLen, (byte) (packed[i] & 0x0F));
        }
    }
}
