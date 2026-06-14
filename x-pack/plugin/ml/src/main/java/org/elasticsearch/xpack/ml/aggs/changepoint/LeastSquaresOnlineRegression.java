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

    LeastSquaresOnlineRegression(int degrees) {
        this.N = degrees + 1;
        statistics = new RunningStatistics(3 * N);
        this.Nx = new Array2DRowRealMatrix(this.N, this.N);
        this.Ny = new Array2DRowRealMatrix(this.N, 1);
        this.Nz = new Array2DRowRealMatrix(this.N, 1);
    }

    public double slopeSign() {
        if (N < 2) {
            return 0.0;
        }
        // Sign of the OLS slope = sign of Cov(x, y) = E[xy] - E[x] E[y]. From statisticAdj the moment
        // layout is E[x] = stats[1], E[y] = stats[2 * N - 1], E[xy] = stats[2 * N].
        return Math.signum(statistics.stats[2 * N] - statistics.stats[1] * statistics.stats[2 * N - 1]);
    }

    public double residualVariance() {
        if (statistics.count <= 0.0) {
            return 0.0;
        }

        // Total variance of y
        double meanY = statistics.stats[2 * N - 1];
        double varRaw = statistics.stats[3 * N - 1] - meanY * meanY;
        double var = Math.max(varRaw, variancePrecisionFloor(meanY));
        double residualVariance = var;
        int n = N + 1;
        boolean done = false;

        // Loop down polynomial degrees until the SVD matrix condition is stable
        while (--n > 0 && done == false) {
            if (n == 1) {
                return var; // Mean-only fit; residual variance is just total variance
            } else if (n == this.N) {
                OptionalDouble maybeResidualVar = residualVariance(N, Nx, Ny, Nz);
                if (maybeResidualVar.isPresent()) {
                    residualVariance = clampResidualVariance(maybeResidualVar.getAsDouble(), var);
                    done = true;
                }
            } else {
                Array2DRowRealMatrix x = new Array2DRowRealMatrix(n, n);
                Array2DRowRealMatrix y = new Array2DRowRealMatrix(n, 1);
                Array2DRowRealMatrix z = new Array2DRowRealMatrix(n, 1);
                OptionalDouble maybeResidualVar = residualVariance(n, x, y, z);
                if (maybeResidualVar.isPresent()) {
                    residualVariance = clampResidualVariance(maybeResidualVar.getAsDouble(), var);
                    done = true;
                }
            }
        }
        return clampResidualVariance(residualVariance, var);
    }

    /**
     * Residual variance of the best polynomial fit of the given {@code degree}, computed from the
     * already-accumulated moments without re-reading the data. The normal-equation system for any
     * degree is a leading principal submatrix of the order-N system, so one accumulation at the
     * maximum degree yields the fit for every degree less than that maximum — removing a factor of
     * {@code degree} from a per-degree model search. Falls back to a lower degree if the requested
     * one is ill-conditioned, matching the behaviour of {@link #residualVariance()}.
     */
    public double residualVarianceForDegree(int degree) {
        if (statistics.count <= 0.0) {
            return 0.0;
        }
        double meanY = statistics.stats[2 * N - 1];
        double varRaw = statistics.stats[3 * N - 1] - meanY * meanY;
        double var = Math.max(varRaw, variancePrecisionFloor(meanY));
        int n = Math.min(Math.max(degree + 1, 1), N) + 1;
        boolean done = false;
        double residualVariance = var;
        while (--n > 0 && done == false) {
            if (n == 1) {
                return var; // Mean-only fit; residual variance is just total variance
            }
            Array2DRowRealMatrix x = new Array2DRowRealMatrix(n, n);
            Array2DRowRealMatrix y = new Array2DRowRealMatrix(n, 1);
            Array2DRowRealMatrix z = new Array2DRowRealMatrix(n, 1);
            OptionalDouble maybeResidualVar = residualVariance(n, x, y, z);
            if (maybeResidualVar.isPresent()) {
                residualVariance = clampResidualVariance(maybeResidualVar.getAsDouble(), var);
                done = true;
            }
        }
        return clampResidualVariance(residualVariance, var);
    }

    public static double variancePrecisionFloor(double mean) {
        // The noise floor for the trend residual variance estimate given it's calculated as E[y^2] - E[y]^2,
        // which carries rounding error of order ulp(mean^2) ~ |mean| * ulp(|mean|).
        double scale = Math.max(Math.abs(mean), 1.0);
        return VARIANCE_PRECISION_ULP_FACTOR * scale * Math.ulp(scale);
    }

    public double rSquared() {
        if (statistics.count <= 0.0) {
            return 0.0;
        }
        double var = statistics.stats[3 * N - 1] - statistics.stats[2 * N - 1] * statistics.stats[2 * N - 1];
        if (var <= 0.0) {
            return 1.0; // Perfect fit if there is zero total variance
        }

        double resVar = residualVariance();
        return Math.min(Math.max(1.0 - resVar / var, 0.0), 1.0);
    }

    private double[] statisticAdj(double x, double y) {
        double[] d = new double[3 * N];
        double xi = 1.0;
        for (int i = 0; i < N; ++i, xi *= x) {
            d[i] = xi;
            d[i + 2 * N - 1] = xi * y;
        }
        for (int i = N; i < 2 * N - 1; ++i, xi *= x) {
            d[i] = xi;
        }
        d[3 * N - 1] = y * y;
        return d;
    }

    void add(double[] yValues, double[] weights, int start, int end) {
        for (int i = start; i < end && i < yValues.length; i++) {
            add(i, yValues[i], weights[i]);
        }
    }

    void remove(double[] yValues, double[] weights, int start, int end) {
        for (int i = start; i < end && i < yValues.length; i++) {
            remove(i, yValues[i], weights[i]);
        }
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
        if (singularValues[0] > SINGLE_VALUE_DECOMPOSITION_MAX_COND * singularValues[n - 1]) {
            return OptionalDouble.empty();
        }
        RealMatrix r = svd.getSolver().solve(y);
        RealMatrix yr = y.transpose().multiply(r);
        RealMatrix zr = z.transpose().multiply(r);
        double t = statistics.stats[2 * N - 1] - zr.getEntry(0, 0);
        double residual = (statistics.stats[3 * N - 1] - yr.getEntry(0, 0)) - (t * t);
        if (Double.isFinite(residual) == false) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(residual);
    }

    private double clampResidualVariance(double residualVariance, double totalVariance) {
        return Math.max(0.0, Math.min(residualVariance, totalVariance));
    }

    private static final double SINGLE_VALUE_DECOMPOSITION_MAX_COND = 1e+12;
    private static final double VARIANCE_PRECISION_ULP_FACTOR = 32.0;

    private final RunningStatistics statistics;
    private final Array2DRowRealMatrix Nx;
    private final Array2DRowRealMatrix Ny;
    private final Array2DRowRealMatrix Nz;
    private final int N;

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
