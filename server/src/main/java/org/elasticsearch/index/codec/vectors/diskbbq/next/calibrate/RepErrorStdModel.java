/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

/**
 * Model for the standard deviation of representation (quantization) error.
 * Uses OLS-predicted log(error_std) as a function of log(nDocsPerCluster) - log(sampleSize).
 * Provides centroid and quantized error std predictions; calibration uses
 * {@link #quantizeRepErrorStd} to predict recall.
 */
public final class RepErrorStdModel {

    private final Regression.OLSResult cparams;
    private final Regression.OLSResult qparams;

    public RepErrorStdModel(Regression.OLSResult cparams, Regression.OLSResult qparams) {
        this.cparams = cparams;
        this.qparams = qparams;
    }

    public Regression.OLSResult cparams() {
        return cparams;
    }

    public Regression.OLSResult qparams() {
        return qparams;
    }

    /**
     * Predicted error std for centroid representation at given cluster/sample sizes.
     */
    public double centroidRepErrorStd(int nDocsPerCluster, int sampleSize) {
        double x = Math.log(nDocsPerCluster) - Math.log(sampleSize);
        Regression.Prediction p = Regression.predictOls(cparams, x);
        return Math.exp(p.mean() + 3.0 * p.std());
    }

    /**
     * Predicted error std for quantized representation; used in the expected recall formula.
     */
    public double quantizeRepErrorStd(int nDocsPerCluster, int sampleSize) {
        double x = Math.log(nDocsPerCluster) - Math.log(sampleSize);
        Regression.Prediction p = Regression.predictOls(qparams, x);
        return Math.exp(p.mean() + 3.0 * p.std());
    }
}
