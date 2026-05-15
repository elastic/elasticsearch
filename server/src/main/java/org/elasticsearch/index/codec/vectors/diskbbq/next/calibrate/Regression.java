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
 * Ordinary least squares regression utilities used by the manifold and error models
 * during quantization calibration.
 */
public final class Regression {

    private Regression() {}

    /**
     * Result of OLS regression: y = beta0 + beta1*x, with coefficient variances
     * and residual variance for prediction intervals.
     *
     * @param beta0   intercept
     * @param beta1   slope
     * @param var0    variance of beta0
     * @param var1    variance of beta1
     * @param cov01   covariance of beta0 and beta1
     * @param sigmaSq residual variance
     */
    public record OLSResult(double beta0, double beta1, double var0, double var1, double cov01, double sigmaSq) {

        public static final OLSResult ZERO = new OLSResult(0, 0, 0, 0, 0, 0);
    }

    /**
     * Point prediction from OLS regression: predicted mean and standard deviation.
     *
     * @param mean predicted value
     * @param std  standard deviation of the prediction
     */
    public record Prediction(double mean, double std) {}

    /**
     * Fit y = beta0 + beta1*x via OLS; returns coefficients, variances, and residual variance.
     */
    public static OLSResult fitOls(double[] x, double[] y) {
        int n = x.length;
        if (n <= 2) {
            return OLSResult.ZERO;
        }
        double sumX = 0, sumY = 0, sumX2 = 0, sumXY = 0;
        for (int i = 0; i < n; i++) {
            sumX += x[i];
            sumY += y[i];
            sumX2 += x[i] * x[i];
            sumXY += x[i] * y[i];
        }
        double xBar = sumX / n;
        double ssXx = sumX2 - (sumX * sumX) / n;
        double ssXy = sumXY - (sumX * sumY) / n;
        if (ssXx == 0) {
            return OLSResult.ZERO;
        }
        double b1 = ssXy / ssXx;
        double b0 = (sumY / n) - b1 * xBar;
        double rss = 0;
        for (int i = 0; i < n; i++) {
            double e = y[i] - (b0 + b1 * x[i]);
            rss += e * e;
        }
        double sSq = rss / (n - 2);
        return new OLSResult(b0, b1, sSq * (sumX2 / (n * ssXx)), sSq / ssXx, sSq * (-xBar / ssXx), sSq);
    }

    /**
     * Predict mean and standard deviation at the given x value using the OLS model.
     */
    public static Prediction predictOls(OLSResult res, double x) {
        double yHat = res.beta0() + res.beta1() * x;
        double varYHat = res.var0() + (x * x * res.var1()) + (2.0 * x * res.cov01());
        double std = Math.sqrt(varYHat + res.sigmaSq());
        return new Prediction(yHat, std);
    }

    /**
     * Coefficient of determination R² for y = beta0 + beta1 * x.
     */
    public static double rSquared(double[] x, double[] y, OLSResult res) {
        int m = x.length;
        double sumY = 0;
        double sumYY = 0;
        double sumRes = 0;
        for (int i = 0; i < m; i++) {
            double yi = y[i];
            double yPred = res.beta0() + res.beta1() * x[i];
            sumY += yi;
            sumYY += yi * yi;
            double e = yi - yPred;
            sumRes += e * e;
        }
        double denom = sumYY - sumY * sumY / m;
        if (denom == 0) {
            return 0;
        }
        return 1.0 - sumRes / denom;
    }
}
