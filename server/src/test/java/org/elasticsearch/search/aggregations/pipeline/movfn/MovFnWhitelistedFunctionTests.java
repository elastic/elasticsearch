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

package org.elasticsearch.search.aggregations.pipeline.movfn;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

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

            double actual = MovingFunctions.windowMax(window.toArray(new Double[]{}));
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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

            double actual = MovingFunctions.windowMin(window.toArray(new Double[]{}));
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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

            double actual = MovingFunctions.windowSum(window.toArray(new Double[]{}));
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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

            double actual = MovingFunctions.simpleMovAvg(window.toArray(new Double[]{}));
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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
            double actual = MovingFunctions.linearMovAvg(window.toArray(new Double[]{}));
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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
            double actual = MovingFunctions.ewmaMovAvg(window.toArray(new Double[]{}), alpha);
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
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
                if (counter == 1) {
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

            double expected = s + (0 * b) ;
            double actual = MovingFunctions.holtMovAvg(window.toArray(new Double[]{}), alpha, beta);
            assertEquals(expected, actual, 0.01 * Math.abs(expected));
            window.offer(randValue);
        }
    }

    public void testHoltWintersMultiplicative() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
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

            seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int idx = window.size() - period + (0 % period);
        double expected = (s + (1 * b)) * seasonal[idx];
        double actual = MovingFunctions.holtWintersMovAvg(window.toArray(new Double[]{}), alpha, beta, gamma, period, true);
        assertEquals(expected, actual, 0.01 * Math.abs(expected));
    }

    public void testHoltWintersAdditive() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);

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

            seasonal[i] = gamma * (vs[i] - (last_s - last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int idx = window.size() - period + (0 % period);
        double expected = s + (1 * b) + seasonal[idx];
        double actual = MovingFunctions.holtWintersMovAvg(window.toArray(new Double[]{}), alpha, beta, gamma, period, false);
        assertEquals(expected, actual, 0.01 * Math.abs(expected));
    }

}
