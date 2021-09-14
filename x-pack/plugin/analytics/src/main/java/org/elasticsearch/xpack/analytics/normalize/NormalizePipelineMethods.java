/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.normalize;

import java.util.function.DoubleUnaryOperator;

class NormalizePipelineMethods {

    // never to be instantiated
    private NormalizePipelineMethods() {}

    static class RescaleZeroToOne extends SinglePassSimpleStatisticsMethod {
        static final String NAME = "rescale_0_1";

        RescaleZeroToOne(double[] values) {
            super(values);
        }

        @Override
        public double applyAsDouble(double value) {
            return (value - min) / (max - min);
        }
    }

    static class RescaleZeroToOneHundred extends SinglePassSimpleStatisticsMethod {
        static final String NAME = "rescale_0_100";

        RescaleZeroToOneHundred(double[] values) {
            super(values);
        }

        @Override
        public double applyAsDouble(double value) {
            return 100 * (value - min) / (max - min);
        }
    }

    static class Mean extends SinglePassSimpleStatisticsMethod {
        static final String NAME = "mean";

        Mean(double[] values) {
            super(values);
        }

        @Override
        public double applyAsDouble(double value) {
            return (value - mean) / (max - min);
        }
    }

    static class Percent extends SinglePassSimpleStatisticsMethod {
        static final String NAME = "percent_of_sum";

        Percent(double[] values) {
            super(values);
        }

        @Override
        public double applyAsDouble(double value) {
            return value / sum;
        }
    }

    static class ZScore extends SinglePassSimpleStatisticsMethod {
        static final String NAME = "z-score";

        private final double stdev;

        ZScore(double[] values) {
            super(values);
            double variance = 0.0;
            for (Double value : values) {
                if (value.isNaN() == false) {
                    variance += Math.pow(value - mean, 2);
                }
            }
            this.stdev = Math.sqrt(variance / count);
        }

        @Override
        public double applyAsDouble(double value) {
            return (value - mean) / stdev;
        }
    }

    static class Softmax implements DoubleUnaryOperator {
        static final String NAME = "softmax";

        private double sumExp;

        Softmax(double[] values) {
            double sumExp = 0.0;
            for (Double value : values) {
                if (value.isNaN() == false) {
                    sumExp += Math.exp(value);
                }
            }

            this.sumExp = sumExp;
        }

        @Override
        public double applyAsDouble(double value) {
            return Math.exp(value) / sumExp;
        }
    }

    abstract static class SinglePassSimpleStatisticsMethod implements DoubleUnaryOperator {
        protected final double max;
        protected final double min;
        protected final double sum;
        protected final double mean;
        protected final int count;

        SinglePassSimpleStatisticsMethod(double[] values) {
            int count = 0;
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (double value : values) {
                if (Double.isNaN(value) == false) {
                    count += 1;
                    min = Math.min(value, min);
                    max = Math.max(value, max);
                    sum += value;
                }
            }

            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.mean = this.count == 0 ? Double.NaN : this.sum / this.count;
        }
    }
}
