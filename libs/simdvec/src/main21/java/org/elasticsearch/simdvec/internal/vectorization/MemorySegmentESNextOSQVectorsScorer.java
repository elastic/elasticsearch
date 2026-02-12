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
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
public final class MemorySegmentESNextOSQVectorsScorer extends ESNextOSQVectorsScorer {

    private final MemorySegmentScorer scorer;

    public MemorySegmentESNextOSQVectorsScorer(
        IndexInput in,
        byte queryBits,
        byte indexBits,
        int dimensions,
        int dataLength,
        int bulkSize,
        MemorySegment memorySegment
    ) {
        super(in, queryBits, indexBits, dimensions, dataLength);
        if (queryBits == 4 && indexBits == 1) {
            this.scorer = new MSBitToInt4ESNextOSQVectorsScorer(in, dimensions, dataLength, bulkSize, memorySegment);
        } else if (queryBits == 4 && indexBits == 4) {
            this.scorer = new MSInt4SymmetricESNextOSQVectorsScorer(in, dimensions, dataLength, bulkSize, memorySegment);
        } else if (queryBits == 4 && indexBits == 2) {
            this.scorer = new MSDibitToInt4ESNextOSQVectorsScorer(in, dimensions, dataLength, bulkSize, memorySegment);
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

    @Override
    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        float score = scorer.scoreBulk(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores,
            bulkSize
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
            scores,
            bulkSize
        );
    }

    abstract static sealed class MemorySegmentScorer permits MSBitToInt4ESNextOSQVectorsScorer, MSDibitToInt4ESNextOSQVectorsScorer,
        MSInt4SymmetricESNextOSQVectorsScorer {

        // TODO: split Panama and Native implementations
        static final boolean NATIVE_SUPPORTED = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();
        static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

        static final float ONE_BIT_SCALE = ESNextOSQVectorsScorer.BIT_SCALES[0];
        static final float TWO_BIT_SCALE = ESNextOSQVectorsScorer.BIT_SCALES[1];
        static final float FOUR_BIT_SCALE = ESNextOSQVectorsScorer.BIT_SCALES[3];

        static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
        static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;

        static final VectorSpecies<Long> LONG_SPECIES_128 = LongVector.SPECIES_128;
        static final VectorSpecies<Long> LONG_SPECIES_256 = LongVector.SPECIES_256;

        static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
        static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;

        static final VectorSpecies<Float> FLOAT_SPECIES_128 = FloatVector.SPECIES_128;
        static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;

        protected final MemorySegment memorySegment;
        protected final IndexInput in;
        protected final int length;
        protected final int dimensions;
        protected final int bulkSize;

        MemorySegmentScorer(IndexInput in, int dimensions, int dataLength, int bulkSize, MemorySegment segment) {
            this.in = in;
            this.length = dataLength;
            this.dimensions = dimensions;
            this.memorySegment = segment;
            this.bulkSize = bulkSize;
        }

        abstract long quantizeScore(byte[] q) throws IOException;

        abstract boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException;

        float scoreBulk(
            byte[] q,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores
        ) throws IOException {
            return scoreBulk(
                q,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                BULK_SIZE
            );
        }

        abstract float scoreBulk(
            byte[] q,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores,
            int bulkSize
        ) throws IOException;

        protected float applyCorrectionsIndividually(
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float indexBitScale,
            float[] scores,
            int bulkSize,
            int limit,
            long offset,
            float ay,
            float ly,
            float y1,
            float maxScore
        ) {
            for (int j = limit; j < bulkSize; j++) {
                float ax = memorySegment.get(
                    ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN),
                    offset + (long) j * Float.BYTES
                );

                float lx = memorySegment.get(
                    ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN),
                    offset + 4L * bulkSize + (long) j * Float.BYTES
                );
                lx = (lx - ax) * indexBitScale;

                int targetComponentSum = memorySegment.get(
                    ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN),
                    offset + 8L * bulkSize + (long) j * Integer.BYTES
                );

                float additionalCorrection = memorySegment.get(
                    ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN),
                    offset + 12L * bulkSize + (long) j * Float.BYTES
                );

                float qcDist = scores[j];

                float res = ax * ay * dimensions + lx * ay * targetComponentSum + ax * ly * y1 + lx * ly * qcDist;

                if (similarityFunction == EUCLIDEAN) {
                    res = res * -2f + additionalCorrection + queryAdditionalCorrection + 1f;
                    res = Math.max(1f / res, 0f);
                    scores[j] = res;
                    maxScore = Math.max(maxScore, res);
                } else {
                    res = res + queryAdditionalCorrection + additionalCorrection - centroidDp;

                    if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                        res = VectorUtil.scaleMaxInnerProductScore(res);
                        scores[j] = res;
                        maxScore = Math.max(maxScore, res);
                    } else {
                        res = Math.max((res + 1f) * 0.5f, 0f);
                        scores[j] = res;
                        maxScore = Math.max(maxScore, res);
                    }
                }
            }
            return maxScore;
        }
    }
}
