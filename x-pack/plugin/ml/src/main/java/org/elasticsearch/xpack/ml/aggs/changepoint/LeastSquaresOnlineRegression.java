/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.util.Arrays;
import java.util.OptionalDouble;

class LeastSquaresOnlineRegression {

    private static final double SINGLE_VALUE_DECOMPOSITION_EPS = 1e+15;

    private final RunningStatistics statistics;
    private final Array2DRowRealMatrix Nx;
    private final Array2DRowRealMatrix Ny;
    private final Array2DRowRealMatrix Nz;
    private final int N;

    LeastSquaresOnlineRegression(int degrees) {
        this.N = degrees + 1;
        statistics = new RunningStatistics(3 * N);
        this.Nx = new Array2DRowRealMatrix(this.N, this.N);
        this.Ny = new Array2DRowRealMatrix(this.N, 1);
        this.Nz = new Array2DRowRealMatrix(this.N, 1);
    }

    double rSquared() {
        double result = 0;
        if (statistics.count <= 0.0) {
            return result;
        }
        double var = statistics.stats[3 * N - 1] - statistics.stats[2 * N - 1] * statistics.stats[2 * N - 1];
        double residualVariance = var;
        int n = N + 1;
        boolean done = false;
        while (--n > 0 && done == false) {
            if (n == 1) {
                return result;
            } else if (n == this.N) {
                OptionalDouble maybeResidualVar = residualVariance(N, Nx, Ny, Nz);
                if (maybeResidualVar.isPresent()) {
                    residualVariance = maybeResidualVar.getAsDouble();
                    done = true;
                }
            } else {
                Array2DRowRealMatrix x = new Array2DRowRealMatrix(n, n);
                Array2DRowRealMatrix y = new Array2DRowRealMatrix(n, 1);
                Array2DRowRealMatrix z = new Array2DRowRealMatrix(n, 1);
                OptionalDouble maybeResidualVar = residualVariance(N, Nx, Ny, Nz);
                if (maybeResidualVar.isPresent()) {
                    residualVariance = maybeResidualVar.getAsDouble();
                    done = true;
                }
            }
        }
        return Math.min(Math.max(1.0 - residualVariance / var, 0.0), 1.0);
    }

    private double[] statisticAdj(double x, double y) {
        double[] d = new double[3 * N];
        double xi = 1.0;
        for (int i = 0; i < N; ++i, xi *= x) {
            d[i] = xi;
            d[i + 2 * N - 1] = xi * y;
        }
        for (int i = 3; i < 2 * N - 1; ++i, xi *= x) {
            d[i] = xi;
        }
        d[3 * N - 1] = y * y;
        return d;
    }

    void add(double x, double y, double weight) {
        statistics.add(statisticAdj(x, y), weight);
    }

    void remove(double x, double y, double weight) {
        statistics.remove(statisticAdj(x, y), weight);
    }

    private OptionalDouble residualVariance(int n, Array2DRowRealMatrix x, Array2DRowRealMatrix y, Array2DRowRealMatrix z) {
        if (n == 1) {
            return OptionalDouble.of(statistics.stats[3 * N - 1] - statistics.stats[2 * N - 1] * statistics.stats[2 * N - 1]);
        }
        for (int i = 0; i < n; ++i) {
            x.setEntry(i, i, statistics.stats[i + i]);
            y.setEntry(i, 0, statistics.stats[i + 2 * N - 1]);
            z.setEntry(i, 0, statistics.stats[i]);
            for (int j = i + 1; j < n; ++j) {
                x.setEntry(i, j, statistics.stats[i + j]);
                x.setEntry(j, i, statistics.stats[i + j]);
            }
        }

        SingularValueDecomposition svd = new SingularValueDecomposition(x);
        double[] singularValues = svd.getSingularValues();
        if (singularValues[0] > SINGLE_VALUE_DECOMPOSITION_EPS * singularValues[n - 1]) {
            return OptionalDouble.empty();
        }
        RealMatrix r = svd.getSolver().solve(y);
        RealMatrix yr = y.transpose().multiply(r);
        RealMatrix zr = z.transpose().multiply(r);
        double t = statistics.stats[2 * N - 1] - zr.getEntry(0, 0);
        return OptionalDouble.of((statistics.stats[3 * N - 1] - yr.getEntry(0, 0)) - (t * t));
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
