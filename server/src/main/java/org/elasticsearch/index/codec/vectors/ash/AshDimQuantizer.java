/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

/**
 * Interface for quantizers used in the ASH projected (latent) space.
 */
public sealed interface AshDimQuantizer permits AshBinaryQuantizer, AshSphericalScalarQuantizer {

    /**
     * Number of bits used per projected dimension.
     *
     * @return the bit width per dimension
     */
    int bitsPerDimension();

    /**
     * Encodes a batch of projected vectors.
     *
     * @param x matrix of shape (n, nDims) in the latent space
     * @return centered codes and their norms
     */
    QuantizeResult encode(float[][] x);

    /**
     * Result of quantization for a single vector.
     *
     * @param centeredCode code centered around zero, length nDims
     * @param codeNorm L2 norm of the code vector
     */
    record SingleQuantizeResult(float[] centeredCode, float codeNorm) {}

    /**
     * Encodes a single projected vector. Default delegates to batch encode.
     *
     * @param xLatent projected vector, length nDims
     * @return centered code and its norm
     */
    default SingleQuantizeResult encodeOne(float[] xLatent) {
        QuantizeResult qr = encode(new float[][] { xLatent });
        return new SingleQuantizeResult(qr.centeredCodes()[0], qr.codeNorms()[0]);
    }

    /**
     * Result of quantization.
     *
     * @param centeredCodes codes centered around zero, shape (n, nDims)
     * @param codeNorms L2 norm of each code vector, length n
     */
    record QuantizeResult(float[][] centeredCodes, float[] codeNorms) {}
}
