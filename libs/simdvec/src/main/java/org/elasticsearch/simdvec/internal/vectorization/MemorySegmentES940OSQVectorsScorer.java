/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.BufferScratch;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Objects;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
public final class MemorySegmentES940OSQVectorsScorer extends ES940OSQVectorsScorer {

    public static MemorySegmentES940OSQVectorsScorer usingNative(
        IndexInput in,
        byte queryBits,
        byte indexBits,
        int dimensions,
        int dataLength,
        int bulkSize,
        @Nullable ES940OSQVectorsScorer.BitEncoding bitEncoding
    ) {
        QuantEncoding encoding = QuantEncoding.of(queryBits, indexBits, Objects.requireNonNullElse(bitEncoding, BitEncoding.STRIPED));
        return new MemorySegmentES940OSQVectorsScorer(
            in,
            encoding,
            dimensions,
            dataLength,
            bulkSize,
            createNativeScorer(encoding, in, dimensions, dataLength, bulkSize)
        );
    }

    public static MemorySegmentES940OSQVectorsScorer usingPanama(
        IndexInput in,
        byte queryBits,
        byte indexBits,
        int dimensions,
        int dataLength,
        int bulkSize,
        @Nullable ES940OSQVectorsScorer.BitEncoding bitEncoding
    ) {
        QuantEncoding encoding = QuantEncoding.of(queryBits, indexBits, Objects.requireNonNullElse(bitEncoding, BitEncoding.STRIPED));
        return new MemorySegmentES940OSQVectorsScorer(
            in,
            encoding,
            dimensions,
            dataLength,
            bulkSize,
            createPanamaScorer(encoding, in, dimensions, dataLength, bulkSize)
        );
    }

    private final MemorySegmentScorer scorer;

    private MemorySegmentES940OSQVectorsScorer(
        IndexInput in,
        QuantEncoding encoding,
        int dimensions,
        int dataLength,
        int bulkSize,
        MemorySegmentScorer scorer
    ) {
        super(in, encoding, dimensions, dataLength, bulkSize);
        this.scorer = scorer;
    }

    private static MemorySegmentScorer createNativeScorer(QuantEncoding enc, IndexInput in, int dimensions, int dataLength, int bulkSize) {
        return switch (enc) {
            case D1Q1, D1Q4, D2Q4_STRIPED, D4Q4_STRIPED -> StripedES940OSQVectorsScorer.usingNative(
                in,
                enc,
                dimensions,
                dataLength,
                bulkSize
            );
            case D2Q4_PACKED -> new NativeD2Q4PackedScorer(in, dimensions, dataLength, bulkSize);
            case D4Q4_PACKED -> new NativeD4Q4PackedScorer(in, dimensions, dataLength, bulkSize);
            case D7Q7 -> new NativeD7Q7Scorer(in, dimensions, dataLength, bulkSize);
        };
    }

    private static MemorySegmentScorer createPanamaScorer(QuantEncoding enc, IndexInput in, int dimensions, int dataLength, int bulkSize) {
        return switch (enc) {
            case D1Q1, D1Q4, D2Q4_STRIPED, D4Q4_STRIPED -> StripedES940OSQVectorsScorer.usingPanama(
                in,
                enc,
                dimensions,
                dataLength,
                bulkSize
            );
            case D2Q4_PACKED -> new MemorySegmentScorer(in, dimensions, dataLength, bulkSize);  // no special implementation yet
            case D4Q4_PACKED -> new MemorySegmentScorer(in, dimensions, dataLength, bulkSize);  // no special implementation yet
            case D7Q7 -> new MSD7Q7ES940OSQVectorsScorer(in, dimensions, dataLength, bulkSize);
        };
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
    public void quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        boolean scored = scorer.quantizeScoreBulkOffsets(q, offsets, offsetsCount, scores, count);
        if (scored == false) {
            super.quantizeScoreBulkOffsets(q, offsets, offsetsCount, scores, count);
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

    @Override
    public float scoreBulkOffsets(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int[] offsets,
        int offsetsCount,
        float[] scores,
        int count
    ) throws IOException {
        float score = scorer.scoreBulkOffsets(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            offsets,
            offsetsCount,
            scores,
            count
        );
        if (score != Float.NEGATIVE_INFINITY) {
            return score;
        }
        return super.scoreBulkOffsets(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            offsets,
            offsetsCount,
            scores,
            count
        );
    }

    static sealed class MemorySegmentScorer permits NativeMemorySegmentScorer, StripedES940OSQVectorsScorer, MSD7Q7ES940OSQVectorsScorer {

        static final float TWO_BIT_SCALE = ES940OSQVectorsScorer.BIT_SCALES[1];
        static final float FOUR_BIT_SCALE = ES940OSQVectorsScorer.BIT_SCALES[3];
        static final float SEVEN_BIT_SCALE = ES940OSQVectorsScorer.BIT_SCALES[6];

        static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
        static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;

        static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

        /** Scale for a quantization of the given bit width, for scorers that are not fixed to one encoding */
        static float bitScale(int bits) {
            return ES940OSQVectorsScorer.BIT_SCALES[bits - 1];
        }

        protected final IndexInput in;
        protected final int length;
        protected final int dimensions;
        protected final int bulkSize;

        protected final BufferScratch scratch = new BufferScratch();

        /**
         * Creates a new MemorySegmentScorer. The index input must be a
         * {@link MemorySegmentAccessInput} or {@link DirectAccessInput};
         * otherwise an {@link IllegalArgumentException} is thrown.
         *
         * <p> Memory segment access is handled by
         * {@link IndexInputUtils#withSlice
         * IndexInputUtils.withSlice}, which probes the index input for
         * {@link MemorySegmentAccessInput} /
         * {@link DirectAccessInput} support and
         * falls back to a heap copy when neither is available.
         *
         * @param in the index input
         * @param dimensions the vector dimensions
         * @param dataLength the length in bytes, per data vector
         * @param bulkSize the number of vectors per bulk
         */
        MemorySegmentScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
            IndexInputUtils.checkInputType(in);
            this.in = in;
            this.length = dataLength;
            this.dimensions = dimensions;
            this.bulkSize = bulkSize;
        }

        /**
         * Re-adjust scores based on the scored offsets positions.
         * Native code uses {@param offsets} to compute {@param offsetsCount} scores, placing them in the first {@param offsetsCount}
         * positions of {@param scores}.
         * This method re-positions them, placing each score at the index indicated in {@param offsets}.
         * @param offsets scored offsets array
         * @param offsetsCount number of scored offsets
         * @param scores scores array
         */
        static void repositionScoresMatchingOffsets(int[] offsets, int offsetsCount, float[] scores) {
            for (int i = offsetsCount - 1; i >= 0; i--) {
                int finalScoreIndex = offsets[i];
                if (i < finalScoreIndex) {
                    scores[finalScoreIndex] = scores[i];
                    scores[i] = 0;
                }
            }
        }

        /**
         * Quantized scoring operation. Returns {@link Long#MIN_VALUE}
         * if this scorer does not implement this scoring.
         */
        long quantizeScore(byte[] q) throws IOException {
            return Long.MIN_VALUE;
        }

        /**
         * Quantized scoring bulk operation. Returns {@code false}
         * if this scorer does not implement this scoring.
         */
        boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
            return false;
        }

        /**
         * Quantized scoring bulk-with-offsets operation. Returns {@code false}
         * if this scorer does not implement this scoring.
         */
        boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
            return false;
        }

        final float scoreBulk(
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
                bulkSize
            );
        }

        /**
         * Score bulk operation. Returns {@link Float#NEGATIVE_INFINITY}
         * if this scorer does not implement this scoring.
         */
        float scoreBulk(
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
            return Float.NEGATIVE_INFINITY;
        }

        /**
         * Score bulk-with-offsets operation. Returns {@link Float#NEGATIVE_INFINITY}
         * if this scorer does not implement this scoring.
         */
        float scoreBulkOffsets(
            byte[] q,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            int[] offsets,
            int offsetsCount,
            float[] scores,
            int count
        ) throws IOException {
            return Float.NEGATIVE_INFINITY;
        }

        protected final float applyCorrectionsBulk(
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores,
            int bulkSize,
            float queryBitScale,
            float indexBitScale
        ) throws IOException {
            return IndexInputUtils.withFloatSlice(
                in,
                16L * bulkSize,
                scratch,
                seg -> applyCorrectionsBulkImpl(
                    seg,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryBitScale,
                    indexBitScale
                )
            );
        }

        private float applyCorrectionsBulkImpl(
            MemorySegment memorySegment,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores,
            int bulkSize,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryBitScale,
            float indexBitScale
        ) {
            int limit = FLOAT_SPECIES.loopBound(bulkSize);
            int i = 0;
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * queryBitScale;
            float y1 = queryComponentSum;
            float maxScore = Float.NEGATIVE_INFINITY;
            for (; i < limit; i += FLOAT_SPECIES.length()) {
                var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, (long) i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
                var lx = FloatVector.fromMemorySegment(
                    FLOAT_SPECIES,
                    memorySegment,
                    4L * bulkSize + (long) i * Float.BYTES,
                    ByteOrder.LITTLE_ENDIAN
                ).sub(ax).mul(indexBitScale);
                var targetComponentSums = IntVector.fromMemorySegment(
                    INT_SPECIES,
                    memorySegment,
                    8L * bulkSize + (long) i * Integer.BYTES,
                    ByteOrder.LITTLE_ENDIAN
                ).convert(VectorOperators.I2F, 0);
                var additionalCorrections = FloatVector.fromMemorySegment(
                    FLOAT_SPECIES,
                    memorySegment,
                    12L * bulkSize + (long) i * Float.BYTES,
                    ByteOrder.LITTLE_ENDIAN
                );
                var qcDist = FloatVector.fromArray(FLOAT_SPECIES, scores, i);
                // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
                var res1 = ax.mul(ay).mul(dimensions);
                var res2 = lx.mul(ay).mul(targetComponentSums);
                var res3 = ax.mul(ly).mul(y1);
                var res4 = lx.mul(ly).mul(qcDist);
                var res = res1.add(res2).add(res3).add(res4);
                switch (similarityFunction) {
                    // For euclidean, we need to invert the score and apply the additional correction, which is
                    // assumed to be the squared l2norm of the centroid centered vectors.
                    case EUCLIDEAN:
                        res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                        res = FloatVector.broadcast(FLOAT_SPECIES, 1).div(res).max(0);
                        maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                        break;
                    // For others, we need to apply the additional correction, which is
                    // assumed to be the non-centered dot-product between the vector and the centroid
                    case MAXIMUM_INNER_PRODUCT:
                        res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                        // see VectorUtil.scaleMaxInnerProductScore
                        var negMask = res.lt(0);
                        var neg = FloatVector.broadcast(FLOAT_SPECIES, 1).div(res.mul(-1).add(1));
                        res = res.add(1).blend(neg, negMask);
                        maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                        break;
                    case COSINE:
                    case DOT_PRODUCT:
                        res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                        res = res.add(1f).mul(0.5f).max(0);
                        maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                        break;
                }
                res.intoArray(scores, i);
            }
            if (limit < bulkSize) {
                maxScore = applyCorrectionsIndividually(
                    memorySegment,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    indexBitScale,
                    scores,
                    bulkSize,
                    limit,
                    ay,
                    ly,
                    y1,
                    maxScore
                );
            }
            return maxScore;
        }

        protected float applyCorrectionsIndividually(
            MemorySegment memorySegment,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float indexBitScale,
            float[] scores,
            int bulkSize,
            int limit,
            float ay,
            float ly,
            float y1,
            float maxScore
        ) {
            for (int j = limit; j < bulkSize; j++) {
                float ax = memorySegment.get(LAYOUT_LE_FLOAT, (long) j * Float.BYTES);

                float lx = memorySegment.get(LAYOUT_LE_FLOAT, 4L * bulkSize + (long) j * Float.BYTES);
                lx = (lx - ax) * indexBitScale;

                int targetComponentSum = memorySegment.get(LAYOUT_LE_INT, 8L * bulkSize + (long) j * Integer.BYTES);

                float additionalCorrection = memorySegment.get(LAYOUT_LE_FLOAT, 12L * bulkSize + (long) j * Float.BYTES);

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
