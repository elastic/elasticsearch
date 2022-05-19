/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class MovFnWhitelistedFunctionTests extends ESTestCase {

    public void testWindowMax() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = -Double.MAX_VALUE;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected = Math.max(expected, value);
            }

            double actual = MovingFunctions.max(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullWindowMax() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.max(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyWindowMax() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.max(window.stream().mapToDouble(Double::doubleValue).toArray());
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testWindowMin() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = Double.MAX_VALUE;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected = Math.min(expected, value);
            }

            double actual = MovingFunctions.min(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullWindowMin() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.min(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyWindowMin() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.min(window.stream().mapToDouble(Double::doubleValue).toArray());
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testWindowSum() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = 0;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected += value;
            }

            double actual = MovingFunctions.sum(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullWindowSum() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.sum(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertThat(actual, equalTo(0.0));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyWindowSum() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.sum(window.stream().mapToDouble(Double::doubleValue).toArray());
        assertThat(actual, equalTo(0.0));
    }

    public void testSimpleMovAvg() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = 0;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected += value;
            }
            expected /= window.size();

            double actual = MovingFunctions.unweightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullSimpleMovAvg() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.unweightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptySimpleMovAvg() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.unweightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testSimpleMovStdDev() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double mean = 0;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                mean += value;
            }
            mean /= window.size();

            double expected = 0.0;
            for (double value : window) {
                expected += Math.pow(value - mean, 2);
            }
            expected = Math.sqrt(expected / window.size());

            double actual = MovingFunctions.stdDev(window.stream().mapToDouble(Double::doubleValue).toArray(), mean);
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullSimpleStdDev() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.stdDev(
                window.stream().mapToDouble(Double::doubleValue).toArray(),
                MovingFunctions.unweightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray())
            );
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptySimpleStdDev() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.stdDev(
            window.stream().mapToDouble(Double::doubleValue).toArray(),
            MovingFunctions.unweightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray())
        );
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testStdDevNaNAvg() {
        assertThat(MovingFunctions.stdDev(new double[] { 1.0, 2.0, 3.0 }, Double.NaN), equalTo(Double.NaN));
    }

    public void testLinearMovAvg() {

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            double expected = avg / totalWeight;
            double actual = MovingFunctions.linearWeightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullLinearMovAvg() {
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.linearWeightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyLinearMovAvg() {
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.linearWeightedAvg(window.stream().mapToDouble(Double::doubleValue).toArray());
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testEWMAMovAvg() {
        double alpha = randomDouble();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            double avg = 0;
            boolean first = true;

            for (double value : window) {
                if (first) {
                    avg = value;
                    first = false;
                } else {
                    avg = (value * alpha) + (avg * (1 - alpha));
                }
            }
            double expected = avg;
            double actual = MovingFunctions.ewma(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha);
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullEwmaMovAvg() {
        double alpha = randomDouble();
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.ewma(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha);
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyEwmaMovAvg() {
        double alpha = randomDouble();
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.ewma(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha);
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testHoltLinearMovAvg() {
        double alpha = randomDouble();
        double beta = randomDouble();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            double s = 0;
            double last_s = 0;

            // Trend value
            double b = 0;
            double last_b = 0;
            int counter = 0;

            double last;
            for (double value : window) {
                last = value;
                if (counter == 0) {
                    s = value;
                    b = value - last;
                } else {
                    s = alpha * value + (1.0d - alpha) * (last_s + last_b);
                    b = beta * (s - last_s) + (1 - beta) * last_b;
                }

                counter += 1;
                last_s = s;
                last_b = b;
            }

            double expected = s + (0 * b);
            double actual = MovingFunctions.holt(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha, beta);
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testNullHoltMovAvg() {
        double alpha = randomDouble();
        double beta = randomDouble();
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            Double randValue = randomBoolean() ? Double.NaN : null;

            if (i == 0) {
                if (randValue != null) {
                    window.offer(randValue);
                }
                continue;
            }

            double actual = MovingFunctions.holt(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha, beta);
            assertThat(actual, equalTo(Double.NaN));
            if (randValue != null) {
                window.offer(randValue);
            }
        }
    }

    public void testEmptyHoltMovAvg() {
        double alpha = randomDouble();
        double beta = randomDouble();
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.holt(window.stream().mapToDouble(Double::doubleValue).toArray(), alpha, beta);
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testHoltWintersMultiplicative() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1, 10);
        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }

        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        // Seasonal value
        double[] seasonal = new double[windowSize];

        int counter = 0;
        double[] vs = new double[windowSize];
        for (double v : window) {
            vs[counter] = v + 0.0000000001;
            counter += 1;
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
            s = alpha * (vs[i] / seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            b = beta * (s - last_s) + (1 - beta) * last_b;

            seasonal[i] = gamma * (vs[i] / (last_s + last_b)) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int idx = window.size() - period + (0 % period);
        double expected = (s + (1 * b)) * seasonal[idx];
        double actual = MovingFunctions.holtWinters(
            window.stream().mapToDouble(Double::doubleValue).toArray(),
            alpha,
            beta,
            gamma,
            period,
            true
        );
        assertEquals(expected, actual, 0.01 * Math.abs(expected));
    }

    public void testNullHoltWintersMovAvg() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1, 10);
        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(Double.NaN);
        }

        for (int i = 0; i < numValues; i++) {
            double actual = MovingFunctions.holtWinters(
                window.stream().mapToDouble(Double::doubleValue).toArray(),
                alpha,
                beta,
                gamma,
                period,
                false
            );
            assertThat(actual, equalTo(Double.NaN));
        }
    }

    public void testEmptyHoltWintersMovAvg() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1, 10);
        EvictingQueue<Double> window = new EvictingQueue<>(0);
        double actual = MovingFunctions.holtWinters(
            window.stream().mapToDouble(Double::doubleValue).toArray(),
            alpha,
            beta,
            gamma,
            period,
            false
        );
        assertThat(actual, equalTo(Double.NaN));
    }

    public void testHoltWintersAdditive() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1, 10);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }

        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        // Seasonal value
        double[] seasonal = new double[windowSize];

        int counter = 0;
        double[] vs = new double[windowSize];
        for (double v : window) {
            vs[counter] = v;
            counter += 1;
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
            s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            b = beta * (s - last_s) + (1 - beta) * last_b;

            seasonal[i] = gamma * (vs[i] - (last_s - last_b)) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int idx = window.size() - period + (0 % period);
        double expected = s + (1 * b) + seasonal[idx];
        double actual = MovingFunctions.holtWinters(
            window.stream().mapToDouble(Double::doubleValue).toArray(),
            alpha,
            beta,
            gamma,
            period,
            false
        );
        assertEquals(expected, actual, 0.01 * Math.abs(expected));
    }

}
