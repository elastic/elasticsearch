/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.simdvec.IndexInputUtils;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    private final int vectorPitch;
    private final Int4VectorScorer.ScorerImpl scorerImpl;
    private final MemorySegment unpackedQuerySegment;
    private final FixedSizeScratch scratch;

    public Int4VectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values, VectorSimilarityType similarityType) {
        IndexInputUtils.checkInputType(input);
        int dims = values.dimension();

        this.input = input;
        this.values = values;
        this.similarityType = similarityType;
        this.packedDims = dims / 2;
        this.vectorPitch = packedDims + Int4VectorScorer.CORRECTIONS_BYTES;
        this.unpackedQuerySegment = Arena.ofAuto().allocate(dims, 32);
        this.scratch = new FixedSizeScratch(vectorPitch);
        float centroidDP;
        try {
            centroidDP = values.getCentroidDP();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.scorerImpl = new Int4VectorScorer.ScorerImpl(
            input,
            values,
            dims,
            packedDims,
            vectorPitch,
            similarityType.function(),
            centroidDP,
            Int4Corrections.singleCorrectionFor(similarityType)
        );
    }

    private Int4VectorScorer.QueryContext createQueryContext(int ord) throws IOException {
        // Read the full per-vector record (packed vector + corrections) in a single slice, instead of a
        // separate getCorrectiveTerms(ord) I/O plus a readBytes for the packed vector. The query nibbles
        // are unpacked straight from the slice, and the corrections are read from its trailing bytes.
        long offset = (long) ord * vectorPitch;
        input.seek(offset);
        return IndexInputUtils.withSlice(input, vectorPitch, scratch::getScratch, seg -> {
            unpackNibbles(seg);
            // The corrections trailer starts at byte offset packedDims within the record; read the
            // 3 floats + 1 int directly from the slice rather than materializing a short-lived sub-slice.
            return new Int4VectorScorer.QueryContext(
                seg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, packedDims),
                seg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, packedDims + Float.BYTES),
                seg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, packedDims + 2L * Float.BYTES),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED, packedDims + 3L * Float.BYTES),
                unpackedQuerySegment
            );
        });
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

    private void unpackNibbles(MemorySegment packed) {
        for (int i = 0; i < packedDims; i++) {
            byte b = packed.get(ValueLayout.JAVA_BYTE, i);
            unpackedQuerySegment.setAtIndex(ValueLayout.JAVA_BYTE, i, (byte) ((b & 0xFF) >>> 4));
            unpackedQuerySegment.setAtIndex(ValueLayout.JAVA_BYTE, i + packedDims, (byte) (b & 0x0F));
        }
    }
}
