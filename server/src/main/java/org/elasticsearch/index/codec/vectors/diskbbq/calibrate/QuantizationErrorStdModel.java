/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

/**
 * Model for the standard deviation of quantization error after scalar quantization.
 * Uses OLS-predicted log(error_std) as a function of log(nDocsPerCluster) - log(sampleSize).
 */
public record QuantizationErrorStdModel(Regression.OLSResult params) {

    /**
     * Predicted error std for quantized representation; used in the expected recall formula.
     */
    public double errorStd(int nDocsPerCluster, int sampleSize) {
        double x = Math.log(nDocsPerCluster) - Math.log(sampleSize);
        Regression.Prediction p = Regression.predictOls(params, x);
        return Math.exp(p.mean() + 3.0 * p.std());
    }
}
