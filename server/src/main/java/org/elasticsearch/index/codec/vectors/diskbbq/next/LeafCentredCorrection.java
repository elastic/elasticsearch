/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;

/**
 * Computes the {@code additionalCorrection} stored for DOT_PRODUCT / MAXIMUM_INNER_PRODUCT float vectors that sit
 * under a parent centroid {@code p}, re-centring the query estimate on the leaf centroid {@code c}.
 *
 * <p>The reader scores {@code est(<q - p, r>) + additionalCorrection + <q, c>} where {@code r = x - c}. Storing
 * {@code <p, r>} (the historical value) makes the integer sum an estimate of {@code <q - p, r>}, whose noise grows with
 * {@code |q - p|}. Storing {@code <c, r> - <c - p, r^>} instead turns the same integer sum into an estimate of
 * {@code <q - c, r>}, which is much smaller for the leaves that hold the query's neighbours. {@code r^ = a + step * k}
 * is the OSQ reconstruction of the residual. The reader, the file layout and the scoring are unchanged.
 *
 * <p>With {@code cp = c - p} this is evaluated without any scalar loop over the dimensions as
 * <pre>{@code
 *   (<x, c> - <c, c>) - a * sum(cp) - step * <cp, k>
 * }</pre>
 * where {@code <x, c>} is the additional correction the quantizer already computed for free while centring the
 * vector, {@code <c, c>}, {@code cp} and {@code sum(cp)} depend only on the (leaf, parent) pair and are computed in
 * {@link #prepare}, and the only per-vector work is narrowing the levels to bytes and one float-by-byte dot product.
 * Callers that process many vectors of one leaf call {@link #prepare} once.
 *
 * <p>Instances hold scratch buffers and are not thread-safe.
 */
final class LeafCentredCorrection {
    private final int dims;
    private final float[] centroidMinusParent;
    private final float[] ones;
    private final byte[] levels;
    private float centroidMinusParentSum;
    private float centroidSquaredNorm;

    LeafCentredCorrection(int dims) {
        this.dims = dims;
        this.centroidMinusParent = new float[dims];
        // summing through a dot product against ones keeps the reduction vectorized; a plain float sum loop is not
        this.ones = new float[dims];
        Arrays.fill(ones, 1f);
        // quantized levels are at most 7 bits wide (see QuantEncoding), so they always fit in a signed byte
        this.levels = new byte[dims];
    }

    /** Sets the (leaf centroid, parent centroid) pair that subsequent {@link #compute} calls refer to. */
    void prepare(float[] centroid, float[] parentCentroid) {
        assert centroid.length == dims && parentCentroid.length == dims;
        ESVectorUtil.subtract(centroid, parentCentroid, centroidMinusParent);
        centroidMinusParentSum = ESVectorUtil.dotProduct(centroidMinusParent, ones);
        centroidSquaredNorm = ESVectorUtil.dotProduct(centroid, centroid);
    }

    /**
     * @param result    the quantizer's result for {@code x} against the leaf centroid; for dot product similarities its
     *                  additional correction is {@code <x, c>}
     * @param quantized the quantized levels {@code k}; only the first {@code dims} entries are read, any
     *                  discretization padding is ignored
     * @param bits      number of bits per quantized level
     */
    float compute(OptimizedScalarQuantizer.QuantizationResult result, int[] quantized, int bits) {
        final float lower = result.lowerInterval();
        final float step = (result.upperInterval() - lower) / ((1 << bits) - 1);
        ESVectorUtil.packAsBytes(quantized, levels, dims);
        float centroidResidual = result.additionalCorrection() - centroidSquaredNorm;
        float shiftLevels = ESVectorUtil.ipFloatByte(centroidMinusParent, levels);
        return centroidResidual - lower * centroidMinusParentSum - step * shiftLevels;
    }
}
