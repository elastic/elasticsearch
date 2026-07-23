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
 * Binary quantizer for ASH: encodes each dimension as its sign bit.
 * Produces codes in {-1, +1} and computes the L2 norm of the code vector.
 */
public final class AshBinaryQuantizer implements AshDimQuantizer {

    @Override
    public int bitsPerDimension() {
        return 1;
    }

    @Override
    public QuantizeResult encode(float[][] x) {
        int n = x.length;
        if (n == 0) {
            return new QuantizeResult(new float[0][0], new float[0]);
        }
        int nDims = x[0].length;
        float[][] centeredCodes = new float[n][nDims];
        float[] codeNorms = new float[n];

        for (int i = 0; i < n; i++) {
            double normSq = 0;
            for (int j = 0; j < nDims; j++) {
                float val = x[i][j] >= 0 ? 1.0f : -1.0f;
                centeredCodes[i][j] = val;
                normSq += val * val;
            }
            codeNorms[i] = (float) Math.sqrt(normSq);
        }
        return new QuantizeResult(centeredCodes, codeNorms);
    }

    @Override
    public SingleQuantizeResult encodeOne(float[] xLatent) {
        int nDims = xLatent.length;
        float[] out = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            out[j] = xLatent[j] >= 0 ? 1.0f : -1.0f;
        }
        // For binary codes, norm = sqrt(nDims) since all values are +/-1
        return new SingleQuantizeResult(out, (float) Math.sqrt(nDims));
    }
}
