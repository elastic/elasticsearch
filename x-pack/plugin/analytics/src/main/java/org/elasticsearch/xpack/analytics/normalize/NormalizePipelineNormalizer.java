/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.normalize;


import java.util.List;

abstract class NormalizePipelineNormalizer {

    static class RescaleZeroToOne extends SinglePassSimpleStatisticsNormalizer {
        static final String NAME = "rescale_0_1";

        RescaleZeroToOne(List<Double> values) {
            super(values);
        }

        @Override
        double normalize(double value) {
            return (value - min) / (max - min);
        }
    }

    static class RescaleZeroToOneHundred extends SinglePassSimpleStatisticsNormalizer {
        static final String NAME = "rescale_0_100";

        RescaleZeroToOneHundred(List<Double> values) {
            super(values);
        }

        @Override
        double normalize(double value) {
            return 100 * (value - min) / (max - min);
        }
    }

    static class Mean extends SinglePassSimpleStatisticsNormalizer {
        static final String NAME = "mean";

        Mean(List<Double> values) {
            super(values);
        }

        @Override
        double normalize(double value) {
            return (value - mean) / (max - min);
        }
    }

    static class Percent extends SinglePassSimpleStatisticsNormalizer {
        static final String NAME = "percent";

        Percent(List<Double> values) {
            super(values);
        }

        @Override
        double normalize(double value) {
            return value / sum;
        }
    }

    static class ZScore extends SinglePassSimpleStatisticsNormalizer {
        static final String NAME = "z-score";

        private final double stdev;

        ZScore(List<Double> values) {
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
        double normalize(double value) {
            return (value - mean) / stdev;
        }
    }

    static class Softmax extends NormalizePipelineNormalizer {
        static final String NAME = "softmax";

        private double sumExp;

        Softmax(List<Double> values) {
            double sumExp = 0.0;
            for (Double value :  values) {
                if (value.isNaN() == false) {
                    sumExp += Math.exp(value);
                }
            }

            this.sumExp = sumExp;
        }

        @Override
        double normalize(double value) {
            return Math.exp(value) / sumExp;
        }
    }

    abstract double normalize(double value);

    abstract static class SinglePassSimpleStatisticsNormalizer extends NormalizePipelineNormalizer {
        protected final double max;
        protected final double min;
        protected final double sum;
        protected final double mean;
        protected final int count;

        SinglePassSimpleStatisticsNormalizer(List<Double> values) {
            int count = 0;
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (Double value :  values) {
                if (value.isNaN() == false) {
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
