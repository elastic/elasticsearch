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

package org.elasticsearch.search.aggregations.pipeline.moving.avg;

import com.google.common.collect.EvictingQueue;

import org.elasticsearch.search.aggregations.pipeline.movavg.models.*;
import org.elasticsearch.test.ElasticsearchTestCase;

import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

import java.util.Arrays;

public class MovAvgUnitTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleMovAvgModel() {
        MovAvgModel model = new SimpleModel();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = 0;

            window.offer(randValue);

            for (double value : window) {
                expected += value;
            }
            expected /= window.size();

            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
        }
    }

    @Test
    public void testSimplePredictionModel() {
        MovAvgModel model = new SimpleModel();

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1,50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);

        double expected[] = new double[numPredictions];
        for (int i = 0; i < numPredictions; i++) {
            for (double value : window) {
                expected[i] += value;
            }
            expected[i] /= window.size();
            window.offer(expected[i]);
        }

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    @Test
    public void testLinearMovAvgModel() {
        MovAvgModel model = new LinearModel();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();
            window.offer(randValue);

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            double expected = avg / totalWeight;
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
        }
    }

    @Test
    public void testLinearPredictionModel() {
        MovAvgModel model = new LinearModel();

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1,50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

        for (int i = 0; i < numPredictions; i++) {
            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            expected[i] = avg / totalWeight;
            window.offer(expected[i]);
        }

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    @Test
    public void testEWMAMovAvgModel() {
        double alpha = randomDouble();
        MovAvgModel model = new EwmaModel(alpha);

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();
            window.offer(randValue);

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
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
        }
    }

    @Test
    public void testEWMAPredictionModel() {
        double alpha = randomDouble();
        MovAvgModel model = new EwmaModel(alpha);

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1,50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

        for (int i = 0; i < numPredictions; i++) {
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
            expected[i] = avg;
            window.offer(expected[i]);
        }

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    @Test
    public void testHoltLinearMovAvgModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        MovAvgModel model = new HoltLinearModel(alpha, beta);

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValues; i++) {
            double randValue = randomDouble();
            window.offer(randValue);

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
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
        }
    }

    @Test
    public void testHoltLinearPredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        MovAvgModel model = new HoltLinearModel(alpha, beta);

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

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

        for (int i = 0; i < numPredictions; i++) {
            expected[i] = s + (i * b);
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    @Test
    public void testHoltWintersMultiplicativePadModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.MULTIPLICATIVE, true);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
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
            b += (vs[i] - vs[i + period]) / 2;
        }
        s /= (double) period;
        b /= (double) period;
        last_s = s;
        last_b = b;

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

            //seasonal[i] = gamma * (vs[i] / s) + ((1 - gamma) * seasonal[i - period]);
            seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int seasonCounter = (windowSize - 1) - period;
        double expected = s + (0 * b) * seasonal[seasonCounter % windowSize];;
        double actual = model.next(window);
        assertThat(Double.compare(expected, actual), equalTo(0));
    }

    @Test
    public void testHoltWintersMultiplicativePadPredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.MULTIPLICATIVE, true);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

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
            b += (vs[i] - vs[i + period]) / 2;
        }
        s /= (double) period;
        b /= (double) period;
        last_s = s;
        last_b = b;

        for (int i = 0; i < period; i++) {
            // Calculate first seasonal
            seasonal[i] = vs[i] / s;
        }

        for (int i = period; i < vs.length; i++) {
            s = alpha * (vs[i] / seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            b = beta * (s - last_s) + (1 - beta) * last_b;

            //seasonal[i] = gamma * (vs[i] / s) + ((1 - gamma) * seasonal[i - period]);
            seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int seasonCounter = (windowSize - 1) - period;

        for (int i = 0; i < numPredictions; i++) {

            expected[i] = s + (i * b) * seasonal[seasonCounter % windowSize];
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
            seasonCounter += 1;
        }

    }

    @Test
    public void testHoltWintersAdditiveModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.ADDITIVE, false);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
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
            b += (vs[i] - vs[i + period]) / 2;
        }
        s /= (double) period;
        b /= (double) period;
        last_s = s;
        last_b = b;

        for (int i = 0; i < period; i++) {
            // Calculate first seasonal
            seasonal[i] = vs[i] / s;
        }

        for (int i = period; i < vs.length; i++) {
            s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            b = beta * (s - last_s) + (1 - beta) * last_b;

            //seasonal[i] = gamma * (vs[i] / s) + ((1 - gamma) * seasonal[i - period]);
            seasonal[i] = gamma * (vs[i] - (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int seasonCounter = (windowSize - 1) - period;
        double expected = s + (0 * b) + seasonal[seasonCounter % windowSize];;
        double actual = model.next(window);
        assertThat(Double.compare(expected, actual), equalTo(0));
    }

    @Test
    public void testHoltWintersAdditivePredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.ADDITIVE, false);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

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
            b += (vs[i] - vs[i + period]) / 2;
        }
        s /= (double) period;
        b /= (double) period;
        last_s = s;
        last_b = b;

        for (int i = 0; i < period; i++) {
            // Calculate first seasonal
            seasonal[i] = vs[i] / s;
        }

        for (int i = period; i < vs.length; i++) {
            s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            b = beta * (s - last_s) + (1 - beta) * last_b;

            //seasonal[i] = gamma * (vs[i] / s) + ((1 - gamma) * seasonal[i - period]);
            seasonal[i] = gamma * (vs[i] - (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            last_s = s;
            last_b = b;
        }

        int seasonCounter = (windowSize - 1) - period;

        for (int i = 0; i < numPredictions; i++) {

            expected[i] = s + (i * b) + seasonal[seasonCounter % windowSize];
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
            seasonCounter += 1;
        }

    }
}
