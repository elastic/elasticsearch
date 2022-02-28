/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.util.Arrays;
import java.util.function.IntToDoubleFunction;

class LeastSquaresOnlineRegression {

    private static final double SINGLE_VALUE_DECOMPOSITION_EPS = 1e+15;

    private final RunningStatistics statistics;
    private final Array2DRowRealMatrix Nx;
    private final Array2DRowRealMatrix Ny;
    private final int N;

    LeastSquaresOnlineRegression(int degrees) {
        this.N = degrees + 1;
        statistics = new RunningStatistics(3 * N - 1);
        this.Nx = new Array2DRowRealMatrix(this.N, this.N);
        this.Ny = new Array2DRowRealMatrix(this.N, 1);
    }

    double squareResidual(double[] x, double[] y, double[] params) {
        return squareResidual(x, 0, x.length, y, 0, y.length, params, i -> 1.0, null);
    }

    double squareResidual(
        double[] x,
        int xOffset,
        int xLength,
        double[] y,
        int yOffset,
        int yLength,
        double[] params,
        IntToDoubleFunction weights,
        Double yVar
    ) {
        if (xLength != yLength) {
            throw new IllegalArgumentException("independent variables and dependent variables must have the same length");
        }
        ChangePointAggregator.RunningStats prediction = new ChangePointAggregator.RunningStats();
        ChangePointAggregator.RunningStats truth = yVar == null ? new ChangePointAggregator.RunningStats() : null;
        for (int i = 0; i < xLength; ++i) {
            double yi = 0;
            double xi = 1;
            for (int j = 0; j < params.length; ++j, xi *= x[i + xOffset]) {
                yi += params[j] * xi;
            }
            prediction.addValue(y[i + yOffset] - yi, weights.applyAsDouble(i + yOffset));
            if (truth != null) {
                truth.addValue(y[i + yOffset], weights.applyAsDouble(i + yOffset));
            }
        }
        double variance = truth != null ? truth.variance() : yVar;
        return 1 - (prediction.variance() / variance);
    }

    void add(double x, double y, double weight) {
        double[] d = new double[3 * N - 1];
        double xi = 1.0;
        for (int i = 0; i < N; ++i, xi *= x) {
            d[i] = xi;
            d[i + 2 * N - 1] = xi * y;
        }
        for (int i = 3; i < 2 * N - 1; ++i, xi *= x) {
            d[i] = xi;
        }
        statistics.add(d, weight);
    }

    void remove(double x, double y, double weight) {
        double[] d = new double[3 * N - 1];
        double xi = 1.0;
        for (int i = 0; i < N; ++i, xi *= x) {
            d[i] = xi;
            d[i + 2 * N - 1] = xi * y;
        }
        for (int i = 3; i < 2 * N - 1; ++i, xi *= x) {
            d[i] = xi;
        }
        statistics.remove(d, weight);
    }

    double[] parameters() {
        double[] results = new double[3];
        if (statistics.count == 0) {
            return results;
        }
        int n = this.N + 1;
        while (--n > 0) {
            if (n == 1) {
                results[0] = statistics.stats[2 * N - 1];
                return results;
            } else if (n == this.N) {
                if (parameters(this.N, Nx, Ny, results)) {
                    return results;
                }
            } else {
                Array2DRowRealMatrix x = new Array2DRowRealMatrix(n, n);
                Array2DRowRealMatrix y = new Array2DRowRealMatrix(n, 1);
                if (parameters(n, x, y, results)) {
                    return results;
                }
            }
        }
        return results;
    }

    private boolean parameters(int n, Array2DRowRealMatrix x, Array2DRowRealMatrix y, double[] result) {
        if (n == 1) {
            result[0] = statistics.stats[2 * N - 1];
            return true;
        }
        for (int i = 0; i < n; ++i) {
            x.setEntry(i, i, statistics.stats[i + i]);
            y.setEntry(i, 0, statistics.stats[i + 2 * N - 1]);
            for (int j = i + 1; j < n; ++j) {
                x.setEntry(i, j, statistics.stats[i + j]);
                x.setEntry(j, i, statistics.stats[i + j]);
            }
        }

        SingularValueDecomposition svd = new SingularValueDecomposition(x);
        double[] singularValues = svd.getSingularValues();
        if (singularValues[0] > SINGLE_VALUE_DECOMPOSITION_EPS * singularValues[n - 1]) {
            return false;
        }
        double[][] data = svd.getSolver().solve(y).getData();
        for (int i = 0; i < n; i++) {
            result[i] = data[i][0];
        }
        return true;
    }

    private static class RunningStatistics {
        private double count;
        private final double[] stats;

        RunningStatistics(int size) {
            count = 0;
            stats = new double[size];
        }

        void add(double[] values, double weight) {
            assert values.length == stats.length
                : "passed values for add are not of expected length; unable to update statistics for online least squares regression";
            count += weight;
            double alpha = weight / count;
            double beta = 1 - alpha;

            for (int i = 0; i < stats.length; i++) {
                stats[i] = stats[i] * beta + alpha * values[i];
            }
        }

        void remove(double[] values, double weight) {
            assert values.length == stats.length
                : "passed values for removal are not of expected length; unable to update statistics for online least squares regression";
            count = Math.max(count - weight, 0);
            if (count == 0) {
                Arrays.fill(stats, 0);
                return;
            }
            double alpha = weight / count;
            double beta = 1 + alpha;

            for (int i = 0; i < stats.length; i++) {
                stats[i] = stats[i] * beta - alpha * values[i];
            }
        }
    }
}
