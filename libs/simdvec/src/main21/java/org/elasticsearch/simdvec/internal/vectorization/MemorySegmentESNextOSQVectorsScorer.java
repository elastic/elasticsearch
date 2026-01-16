/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
public final class MemorySegmentESNextOSQVectorsScorer extends ESNextOSQVectorsScorer {

    private final MemorySegment memorySegment;
    private final MemorySegmentScorer scorer;

    public MemorySegmentESNextOSQVectorsScorer(
        IndexInput in,
        byte queryBits,
        byte indexBits,
        int dimensions,
        int dataLength,
        MemorySegment memorySegment
    ) {
        super(in, queryBits, indexBits, dimensions, dataLength);
        this.memorySegment = memorySegment;
        if (queryBits == 4 && indexBits == 1) {
            this.scorer = new MSBitToInt4ESNextOSQVectorsScorer(in, dimensions, dataLength, memorySegment);
        } else if (queryBits == 4 && indexBits == 4) {
            this.scorer = new MSInt4SymmetricESNextOSQVectorsScorer(in, dimensions, dataLength, memorySegment);
        } else if (queryBits == 4 && indexBits == 2) {
            this.scorer = new MSDibitToInt4ESNextOSQVectorsScorer(in, dimensions, dataLength, memorySegment);
        } else {
            throw new IllegalArgumentException("Only asymmetric 4-bit query and 1-bit index supported");
        }
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        long score = scorer.quantizeScore(q);
        if (score != Long.MIN_VALUE) {
            return score;
        }
        return super.quantizeScore(q);
    }

    @Override
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        boolean scored = scorer.quantizeScoreBulk(q, count, scores);
        if (scored == false) {
            super.quantizeScoreBulk(q, count, scores);
        }
    }

    @Override
    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        float score = scorer.scoreBulk(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores
        );
        if (score != Float.NEGATIVE_INFINITY) {
            return score;
        }
        return super.scoreBulk(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores
        );
    }

    abstract static sealed class MemorySegmentScorer permits MSBitToInt4ESNextOSQVectorsScorer, MSDibitToInt4ESNextOSQVectorsScorer,
        MSInt4SymmetricESNextOSQVectorsScorer {

        static final int BULK_SIZE = MemorySegmentESNextOSQVectorsScorer.BULK_SIZE;
        static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);
        static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;

        static final VectorSpecies<Long> LONG_SPECIES_128 = LongVector.SPECIES_128;
        static final VectorSpecies<Long> LONG_SPECIES_256 = LongVector.SPECIES_256;

        static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
        static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;

        static final VectorSpecies<Short> SHORT_SPECIES_128 = ShortVector.SPECIES_128;
        static final VectorSpecies<Short> SHORT_SPECIES_256 = ShortVector.SPECIES_256;

        static final VectorSpecies<Float> FLOAT_SPECIES_128 = FloatVector.SPECIES_128;
        static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;

        protected final MemorySegment memorySegment;
        protected final IndexInput in;
        protected final int length;
        protected final int dimensions;

        MemorySegmentScorer(IndexInput in, int dimensions, int dataLength, MemorySegment segment) {
            this.in = in;
            this.length = dataLength;
            this.dimensions = dimensions;
            this.memorySegment = segment;
        }

        abstract long quantizeScore(byte[] q) throws IOException;

        abstract boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException;

        abstract float scoreBulk(
            byte[] q,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores
        ) throws IOException;
    }
}
