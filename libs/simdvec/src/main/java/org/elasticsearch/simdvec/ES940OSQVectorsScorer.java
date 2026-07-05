/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.internal.vectorization.JdkFeatures;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Scorer for quantized vectors stored as an {@link IndexInput}. */
public class ES940OSQVectorsScorer {

    public static final int BULK_SIZE = 32;

    public static boolean supportsQuantization(byte queryBits, byte indexBits) {
        return switch ((queryBits << 8) | indexBits) {
            case (1 << 8) | 1, (4 << 8) | 1, (4 << 8) | 2, (4 << 8) | 4, (7 << 8) | 7 -> true;
            default -> false;
        };
    }

    public enum BitEncoding {
        STRIPED,
        PACKED
    }

    public enum QuantEncoding {
        D1Q1((byte) 1, (byte) 1),
        D1Q4((byte) 1, (byte) 4),
        D2Q4_STRIPED((byte) 2, (byte) 4),
        D2Q4_PACKED((byte) 2, (byte) 4, BitEncoding.PACKED),
        D4Q4_STRIPED((byte) 4, (byte) 4),
        D4Q4_PACKED((byte) 4, (byte) 4, BitEncoding.PACKED),
        D7Q7((byte) 7, (byte) 7);

        private final byte indexBits;
        private final byte queryBits;
        private final BitEncoding bitEncoding;

        QuantEncoding(byte indexBits, byte queryBits) {
            this(indexBits, queryBits, BitEncoding.STRIPED);
        }

        QuantEncoding(byte indexBits, byte queryBits, BitEncoding bitEncoding) {
            this.indexBits = indexBits;
            this.queryBits = queryBits;
            this.bitEncoding = bitEncoding;
        }

        public static QuantEncoding of(byte queryBits, byte indexBits, BitEncoding bitEncoding) {
            return switch ((queryBits << 8) | indexBits) {
                case (1 << 8) | 1 -> D1Q1;
                case (4 << 8) | 1 -> D1Q4;
                case (4 << 8) | 2 -> bitEncoding == BitEncoding.PACKED ? D2Q4_PACKED : D2Q4_STRIPED;
                case (4 << 8) | 4 -> bitEncoding == BitEncoding.PACKED ? D4Q4_PACKED : D4Q4_STRIPED;
                case (7 << 8) | 7 -> D7Q7;
                default -> throw new IllegalArgumentException("Unsupported query/index bits combination: " + queryBits + "/" + indexBits);
            };
        }

        public byte indexBits() {
            return indexBits;
        }

        public byte queryBits() {
            return queryBits;
        }

        public BitEncoding bitEncoding() {
            return bitEncoding;
        }
    }

    protected static final float[] BIT_SCALES = new float[] {
        1f,
        1f / ((1 << 2) - 1),
        1f / ((1 << 3) - 1),
        1f / ((1 << 4) - 1),
        1f / ((1 << 5) - 1),
        1f / ((1 << 6) - 1),
        1f / ((1 << 7) - 1),
        1f / ((1 << 8) - 1), };

    /** The wrapper {@link IndexInput}. */
    protected final IndexInput in;

    protected final QuantEncoding encoding;
    protected final int length;
    protected final int dimensions;
    protected final int bulkSize;

    protected final float[] lowerIntervals;
    protected final float[] upperIntervals;
    protected final int[] targetComponentSums;
    protected final float[] additionalCorrections;
    private final byte[] scratch;
    private final byte[] packedScratch;

    public ES940OSQVectorsScorer(IndexInput in, QuantEncoding encoding, int dimensions, int dataLength, int bulkSize) {
        this.in = in;
        this.encoding = encoding;
        this.dimensions = dimensions;
        this.length = dataLength;
        this.lowerIntervals = new float[bulkSize];
        this.upperIntervals = new float[bulkSize];
        this.targetComponentSums = new int[bulkSize];
        this.additionalCorrections = new float[bulkSize];
        this.bulkSize = bulkSize;
        this.scratch = encoding.indexBits() == 7 ? new byte[dimensions] : null;
        this.packedScratch = encoding.bitEncoding == BitEncoding.PACKED ? new byte[length] : null;
    }

    public ES940OSQVectorsScorer(
        IndexInput in,
        byte queryBits,
        byte indexBits,
        int dimensions,
        int dataLength,
        int bulkSize,
        BitEncoding bitEncoding
    ) {
        this(in, QuantEncoding.of(queryBits, indexBits, bitEncoding), dimensions, dataLength, bulkSize);
    }

    public ES940OSQVectorsScorer(IndexInput in, byte queryBits, byte indexBits, int dimensions, int dataLength, int bulkSize) {
        this(in, queryBits, indexBits, dimensions, dataLength, bulkSize, BitEncoding.STRIPED);
    }

    public ES940OSQVectorsScorer(IndexInput in, byte queryBits, byte indexBits, int dimensions, int dataLength) {
        this(in, queryBits, indexBits, dimensions, dataLength, BULK_SIZE);
    }

    /**
     * compute the quantize distance between the provided quantized query and the quantized vector
     * that is read from the wrapped {@link IndexInput}.
     */
    public long quantizeScore(byte[] q) throws IOException {
        return switch (encoding) {
            case D1Q1 -> quantized1BitScoreSymmetric(q, length);
            case D1Q4 -> quantized4BitScore(q, length);
            case D2Q4_STRIPED -> quantized4BitScore2BitIndexStriped(q);
            case D2Q4_PACKED -> quantized4BitScore2BitIndexPacked(q);
            case D4Q4_PACKED -> quantized4BitScorePacked(q);
            case D4Q4_STRIPED -> quantized4BitScoreSymmetric(q);
            case D7Q7 -> quantized7BitScore(q);
        };
    }

    private long quantized1BitScoreSymmetric(byte[] q, int length) throws IOException {
        assert q.length == length : "length mismatch q " + q.length + " vs " + length;
        long score = 0;
        for (int i = 0; i < length; i++) {
            score += Long.bitCount((q[i] & in.readByte()) & 0xFF);
        }
        return score;
    }

    private long quantized7BitScore(byte[] q) throws IOException {
        in.readBytes(scratch, 0, dimensions);
        return VectorUtil.dotProduct(scratch, q);
    }

    private long quantized4BitScoreSymmetric(byte[] q) throws IOException {
        assert q.length == length : "length mismatch q " + q.length + " vs " + length;
        assert length % 4 == 0 : "length must be multiple of 4 for 4-bit index length: " + length + " dimensions: " + dimensions;
        int stripe0 = (int) quantized4BitScore(q, length / 4);
        int stripe1 = (int) quantized4BitScore(q, length / 4);
        int stripe2 = (int) quantized4BitScore(q, length / 4);
        int stripe3 = (int) quantized4BitScore(q, length / 4);
        return stripe0 + ((long) stripe1 << 1) + ((long) stripe2 << 2) + ((long) stripe3 << 3);
    }

    private long quantized4BitScorePacked(byte[] q) throws IOException {
        assert q.length == length * 2 : "length mismatch q " + q.length + " vs " + (length * 2);
        in.readBytes(packedScratch, 0, length);
        if (JdkFeatures.SUPPORTS_HEAP_SEGMENTS) {
            return VectorUtil.int4DotProductSinglePacked(q, packedScratch);
        }
        long score = 0;
        for (int i = 0; i < length; i++) {
            int packed = packedScratch[i] & 0xFF;
            score += ((packed >>> 4) & 0x0F) * (q[i] & 0x0F);
            score += (packed & 0x0F) * (q[i + length] & 0x0F);
        }
        return score;
    }

    private long quantized4BitScore2BitIndexStriped(byte[] q) throws IOException {
        assert q.length == length * 2;
        assert length % 2 == 0 : "length must be even for 2-bit index length: " + length + " dimensions: " + dimensions;
        int lower = (int) quantized4BitScore(q, length / 2);
        int upper = (int) quantized4BitScore(q, length / 2);
        return lower + ((long) upper << 1);
    }

    private long quantized4BitScore2BitIndexPacked(byte[] q) throws IOException {
        assert q.length == length * 4 : "length mismatch q " + q.length + " vs " + (length * 4);
        in.readBytes(packedScratch, 0, length);
        long score = 0;
        for (int i = 0; i < length; i++) {
            int packed = packedScratch[i] & 0xFF;
            score += ((packed >>> 6) & 0x03) * (q[i] & 0x0F);
            score += ((packed >>> 4) & 0x03) * (q[i + length] & 0x0F);
            score += ((packed >>> 2) & 0x03) * (q[i + 2 * length] & 0x0F);
            score += (packed & 0x03) * (q[i + 3 * length] & 0x0F);
        }
        return score;
    }

    private long quantized4BitScore(byte[] q, int length) throws IOException {
        assert q.length == length * 4;
        final int size = length;
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int r = 0;
        for (final int upperBound = size & -Long.BYTES; r < upperBound; r += Long.BYTES) {
            final long value = in.readLong();
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
            final int value = in.readInt();
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 3 * size) & value);
        }
        for (; r < size; r++) {
            final byte value = in.readByte();
            subRet0 += Integer.bitCount((q[r] & value) & 0xFF);
            subRet1 += Integer.bitCount((q[r + size] & value) & 0xFF);
            subRet2 += Integer.bitCount((q[r + 2 * size] & value) & 0xFF);
            subRet3 += Integer.bitCount((q[r + 3 * size] & value) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    /**
     * Compute the quantize distance between the provided quantized query and the quantized vectors
     * that are read from the wrapped {@link IndexInput}. The number of quantized vectors to read is
     * determined by {@param count} and the results are stored in the provided {@param scores} array.
     */
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = quantizeScore(q);
        }
    }

    /**
     * Compute the quantize distance between the provided quantized query and the quantized vectors
     * that are read from the wrapped {@link IndexInput}. The number of quantized vectors to read is
     * determined by {@param count} and the results are stored in the provided {@param scores} array.
     * Only the {@param offsetsCount} vectors that are indexed in the provided {@param offsets} are scored; the others are skipped.
     */
    public void quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        int offsetIndex = 0;
        for (int i = 0; i < count; i++) {
            if (offsetIndex < offsetsCount && offsets[offsetIndex] == i) {
                offsetIndex++;
                scores[i] = quantizeScore(q);
            } else {
                scores[i] = 0.0f;
                in.skipBytes(length);
            }
        }
    }

    /**
     * Computes the final score by applying the necessary corrections to the provided quantized distance.
     */
    public float applyCorrectionsIndividually(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float lowerInterval,
        float upperInterval,
        int targetComponentSum,
        float additionalCorrection,
        float qcDist
    ) {
        float ax = lowerInterval;
        // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
        float lx = (upperInterval - ax) * BIT_SCALES[encoding.indexBits() - 1];
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * BIT_SCALES[encoding.queryBits() - 1];
        float y1 = queryComponentSum;
        float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        if (similarityFunction == EUCLIDEAN) {
            score = queryAdditionalCorrection + additionalCorrection - 2 * score;
            return Math.max(1 / (1f + score), 0);
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryAdditionalCorrection + additionalCorrection - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return Math.max((1f + score) / 2f, 0);
        }
    }

    /**
     * Bulk score overload; same as {@link #scoreBulk(byte[], float, float, int, float, VectorSimilarityFunction, float, float[], int)}
     * with {@link #bulkSize} as the default {@code bulkSize} param value.
     */
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
     * Compute the distance between the provided quantized query and the quantized vectors that are
     * read from the wrapped {@link IndexInput}.
     *
     * <p>The number of vectors to score is defined by {@param bulkSize}. The expected format of the
     * input is as follows: First the quantized vectors are read from the input,then all the lower
     * intervals as floats, then all the upper intervals as floats, then all the target component sums
     * as shorts, and finally all the additional corrections as floats.
     *
     * <p>The results are stored in the provided {@param scores} array.
     */
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
        assert bulkSize <= this.bulkSize : "supplied bulkSize > this scorer's bulkSize";
        quantizeScoreBulk(q, bulkSize, scores);
        in.readFloats(lowerIntervals, 0, bulkSize);
        in.readFloats(upperIntervals, 0, bulkSize);
        in.readInts(targetComponentSums, 0, bulkSize);
        in.readFloats(additionalCorrections, 0, bulkSize);
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < bulkSize; i++) {
            scores[i] = applyCorrectionsIndividually(
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                lowerIntervals[i],
                upperIntervals[i],
                targetComponentSums[i],
                additionalCorrections[i],
                scores[i]
            );
            if (scores[i] > maxScore) {
                maxScore = scores[i];
            }
        }
        return maxScore;
    }

    /**
     * Compute the distance between the provided quantized query and the quantized vectors that are
     * read from the wrapped {@link IndexInput}.
     * <p>
     * Similar to {@link #scoreBulk}, but only the {@param offsetsCount} vectors indexed by the provided {@param offsets} are scored;
     * the others are skipped.
     * <p>
     * The results are stored in the provided {@param scores} array.
     */
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
        assert offsetsCount <= this.bulkSize : "supplied offsets length > this scorer's bulkSize";
        assert count <= this.bulkSize : "supplied count > this scorer's bulkSize";
        quantizeScoreBulkOffsets(q, offsets, offsetsCount, scores, count);
        in.readFloats(lowerIntervals, 0, count);
        in.readFloats(upperIntervals, 0, count);
        in.readInts(targetComponentSums, 0, count);
        in.readFloats(additionalCorrections, 0, count);
        float maxScore = Float.NEGATIVE_INFINITY;
        int offsetIndex = 0;
        for (int i = 0; i < count; i++) {
            if (offsetIndex < offsetsCount && offsets[offsetIndex] == i) {
                offsetIndex++;
                scores[i] = applyCorrectionsIndividually(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    lowerIntervals[i],
                    upperIntervals[i],
                    targetComponentSums[i],
                    additionalCorrections[i],
                    scores[i]
                );
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            }
        }
        return maxScore;
    }
}
