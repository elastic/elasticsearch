/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import java.util.Arrays;

/**
 * Provides a collection of static utility methods that can be referenced from MovingFunction script contexts
 */
public class MovingFunctions {

    /**
     * Find the maximum value in a window of values.
     * If all values are missing/null/NaN, the return value will be NaN
     */
    public static double max(double[] values) {
        return Arrays.stream(values).max().orElse(Double.NaN);
    }

    /**
     * Find the minimum value in a window of values
     * If all values are missing/null/NaN, the return value will be NaN
     */
    public static double min(double[] values) {
        return Arrays.stream(values).min().orElse(Double.NaN);
    }

    /**
     * Find the sum of a window of values
     * If all values are missing/null/NaN, the return value will be 0.0
     */
    public static double sum(double[] values) {
        if (values.length == 0) {
            return 0.0;
        }
        return Arrays.stream(values).map(value -> {
            if (Double.isNaN(value) == false) {
                return value;
            }
            return 0.0;
        }).sum();
    }

    /**
     * Calculate a simple unweighted (arithmetic) moving average.
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN.
     * The average is based on the count of non-null, non-NaN values.
     */
    public static double unweightedAvg(double[] values) {
        double avg = 0.0;
        long count = 0;
        for (double v : values) {
            if (Double.isNaN(v) == false) {
                avg += v;
                count += 1;
            }
        }
        return count == 0 ? Double.NaN : avg / count;
    }

    /**
     * Calculate a standard deviation over the values using the provided average.
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN.
     * The average is based on the count of non-null, non-NaN values.
     */
    public static double stdDev(double[] values, double avg) {
        if (Double.isNaN(avg)) {
            return Double.NaN;
        } else {
            long count = 0;
            double squaredMean = 0;
            for (double v : values) {
                if (Double.isNaN(v) == false) {
                    squaredMean += Math.pow(v - avg, 2);
                    count += 1;
                }
            }
            return Math.sqrt(squaredMean / count);
        }
    }

    /**
     * Calculate a linearly weighted moving average, such that older values are
     * linearly less important.  "Time" is determined by position in collection
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN
     * The average is based on the count of non-null, non-NaN values.
     */
    public static double linearWeightedAvg(double[] values) {
        double avg = 0;
        long totalWeight = 1;
        long current = 1;

        for (double v : values) {
            if (Double.isNaN(v) == false) {
                avg += v * current;
                totalWeight += current;
                current += 1;
            }
        }
        return totalWeight == 1 ? Double.NaN : avg / totalWeight;
    }

    /**
     *
     * Calculate a exponentially weighted moving average.
     *
     * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
     * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN
     * The average is based on the count of non-null, non-NaN values.
     *
     * @param alpha A double between 0-1 inclusive, controls data smoothing
     */
    public static double ewma(double[] values, double alpha) {
        double avg = Double.NaN;
        boolean first = true;

        for (double v : values) {
            if (Double.isNaN(v) == false) {
                if (first) {
                    avg = v;
                    first = false;
                } else {
                    avg = (v * alpha) + (avg * (1 - alpha));
                }
            }
        }
        return avg;
    }

    /**
     * Calculate a doubly exponential weighted moving average
     *
     * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
     * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
     *
     * Beta is equivalent to alpha, but controls the smoothing of the trend instead of the data
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN
     * The average is based on the count of non-null, non-NaN values.
     *
     * @param alpha A double between 0-1 inclusive, controls data smoothing
     * @param beta a double between 0-1 inclusive, controls trend smoothing
     */
    public static double holt(double[] values, double alpha, double beta) {
        if (values.length == 0) {
            return Double.NaN;
        }

        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        int counter = 0;

        Double last;
        for (double v : values) {
            if (Double.isNaN(v) == false) {
                last = v;
                if (counter == 0) {
                    s = v;
                    b = v - last;
                } else {
                    s = alpha * v + (1.0d - alpha) * (last_s + last_b);
                    b = beta * (s - last_s) + (1 - beta) * last_b;
                }

                counter += 1;
                last_s = s;
                last_b = b;
            }
        }

        if (counter == 0) {
            return Double.NaN;
        }

        return s;
    }

    /**
     * Calculate a triple exponential weighted moving average
     *
     * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
     * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
     *
     * Beta is equivalent to alpha, but controls the smoothing of the trend instead of the data.
     * Gamma is equivalent to alpha, but controls the smoothing of the seasonality instead of the data
     *
     * Only finite values are averaged.  NaN or null are ignored.
     * If all values are missing/null/NaN, the return value will be NaN
     * The average is based on the count of non-null, non-NaN values.
     *
     * @param alpha A double between 0-1 inclusive, controls data smoothing
     * @param beta a double between 0-1 inclusive, controls trend smoothing
     * @param gamma a double between 0-1 inclusive, controls seasonality smoothing
     * @param period the expected periodicity of the data
     * @param multiplicative true if multiplicative HW should be used. False for additive
     */
    public static double holtWinters(double[] values, double alpha, double beta, double gamma,
                                           int period, boolean multiplicative) {

        if (values.length == 0) {
            return Double.NaN;
        }

        double padding = multiplicative ? 0.0000000001 : 0.0;
        if (values.length < period * 2) {
            // We need at least two full "seasons" to use HW
            // This should have been caught earlier, we can't do anything now...bail
            throw new IllegalArgumentException("Holt-Winters aggregation requires at least (2 * period == 2 * "
                + period + " == "+(2 * period)+") data-points to function.  Only [" + values.length + "] were provided.");
        }

        // Smoothed value
        double s = 0;
        double last_s;

        // Trend value
        double b = 0;
        double last_b = 0;

        // Seasonal value
        double[] seasonal = new double[values.length];

        int counter = 0;
        double[] vs = new double[values.length];
        for (double v : values) {
            if (Double.isNaN(v) == false) {
                vs[counter] = v + padding;
                counter += 1;
            }
        }

        if (counter == 0) {
            return Double.NaN;
        }

        // Initial level value is average of first season
        // Calculate the slopes between first and second season for each period
        for (int i = 0; i < period; i++) {
            s += vs[i];
            b += (vs[i + period] - vs[i]) / period;
        }
        s /= period;
        b /= period;
        last_s = s;

        // Calculate first seasonal
        if (Double.compare(s, 0.0) == 0 || Double.compare(s, -0.0) == 0) {
            Arrays.fill(seasonal, 0.0);
        } else {
            for (int i = 0; i < period; i++) {
                seasonal[i] = vs[i] / s;
            }
        }

        for (int i = period; i < vs.length; i++) {
            // TODO if perf is a problem, we can specialize a subclass to avoid conditionals on each iteration
            if (multiplicative) {
                s = alpha * (vs[i] / seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            } else {
                s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            }

            b = beta * (s - last_s) + (1 - beta) * last_b;

            if (multiplicative) {
                seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            } else {
                seasonal[i] = gamma * (vs[i] - (last_s - last_b )) + (1 - gamma) * seasonal[i - period];
            }

            last_s = s;
            last_b = b;
        }

        int idx = values.length - period;
        if (multiplicative) {
            return (s + b) * seasonal[idx];
        }
        return s + b + seasonal[idx];
    }
}
