/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

public interface ESVectorUtilSupport {

    /**
     * The number of bits in bit-quantized query vectors
     */
    short B_QUERY = 4;

    /**
     * Compute dot product between {@code q} and {@code d}
     * @param q query vector, {@link #B_QUERY}-bit quantized and striped (see {@code BQSpaceUtils.transposeHalfByte})
     * @param d data vector, 1-bit quantized
     */
    long ipByteBinByte(byte[] q, byte[] d);

    int ipByteBit(byte[] q, byte[] d);

    float ipFloatBit(float[] q, byte[] d);

    float ipFloatByte(float[] q, byte[] d);

    float calculateOSQLoss(float[] target, float[] interval, float step, float invStep, float norm2, float lambda);

    void calculateOSQGridPoints(float[] target, float[] interval, int points, float invStep, float[] pts);

    void centerAndCalculateOSQStatsEuclidean(float[] target, float[] centroid, float[] centered, float[] stats);

    void centerAndCalculateOSQStatsDp(float[] target, float[] centroid, float[] centered, float[] stats);

    float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm);

    int quantizeVectorWithIntervals(float[] vector, int[] quantize, float lowInterval, float upperInterval, byte bit);

}
