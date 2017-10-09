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

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MovAvgUnitTests extends ESTestCase {
    public void testSimpleMovAvgModel() {
        MovAvgModel model = new SimpleModel();

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

            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
            window.offer(randValue);
        }
    }

    public void testSimplePredictionModel() {
        MovAvgModel model = new SimpleModel();

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);

        double expected[] = new double[numPredictions];
        double t = 0;
        for (double value : window) {
            t += value;
        }
        t /= window.size();
        Arrays.fill(expected, t);

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    public void testLinearMovAvgModel() {
        MovAvgModel model = new LinearModel();

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
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
            window.offer(randValue);
        }
    }

    public void testLinearPredictionModel() {
        MovAvgModel model = new LinearModel();

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1,50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

        double avg = 0;
        long totalWeight = 1;
        long current = 1;

        for (double value : window) {
            avg += value * current;
            totalWeight += current;
            current += 1;
        }
        avg = avg / totalWeight;
        Arrays.fill(expected, avg);

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    public void testEWMAMovAvgModel() {
        double alpha = randomDouble();
        MovAvgModel model = new EwmaModel(alpha);

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
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
            window.offer(randValue);
        }
    }

    public void testEWMAPredictionModel() {
        double alpha = randomDouble();
        MovAvgModel model = new EwmaModel(alpha);

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1,50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < windowSize; i++) {
            window.offer(randomDouble());
        }
        double actual[] = model.predict(window, numPredictions);
        double expected[] = new double[numPredictions];

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
        Arrays.fill(expected, avg);

        for (int i = 0; i < numPredictions; i++) {
            assertThat(Double.compare(expected[i], actual[i]), equalTo(0));
        }
    }

    public void testHoltLinearMovAvgModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        MovAvgModel model = new HoltLinearModel(alpha, beta);

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
            double actual = model.next(window);
            assertThat(Double.compare(expected, actual), equalTo(0));
            window.offer(randValue);
        }
    }

    public void testHoltLinearPredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        MovAvgModel model = new HoltLinearModel(alpha, beta);

        int windowSize = randomIntBetween(1, 50);
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
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

    public void testHoltWintersMultiplicativePadModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.MULTIPLICATIVE, true);

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
        double actual = model.next(window);
        assertThat(Double.compare(expected, actual), equalTo(0));
    }

    public void testHoltWintersMultiplicativePadPredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.MULTIPLICATIVE, true);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
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


        for (int i = 1; i <= numPredictions; i++) {
            int idx = window.size() - period + ((i - 1) % period);
            expected[i-1] = (s + (i * b)) * seasonal[idx];
            assertThat(Double.compare(expected[i-1], actual[i-1]), equalTo(0));
        }

    }

    public void testHoltWintersAdditiveModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.ADDITIVE, false);

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
        double actual = model.next(window);
        assertThat(Double.compare(expected, actual), equalTo(0));
    }

    public void testHoltWintersAdditivePredictionModel() {
        double alpha = randomDouble();
        double beta = randomDouble();
        double gamma = randomDouble();
        int period = randomIntBetween(1,10);
        MovAvgModel model = new HoltWintersModel(alpha, beta, gamma, period, HoltWintersModel.SeasonalityType.ADDITIVE, false);

        int windowSize = randomIntBetween(period * 2, 50); // HW requires at least two periods of data
        int numPredictions = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
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

        for (int i = 1; i <= numPredictions; i++) {
            int idx = window.size() - period + ((i - 1) % period);
            expected[i-1] = s + (i * b) + seasonal[idx];
            assertThat(Double.compare(expected[i-1], actual[i-1]), equalTo(0));
        }

    }

    public void testNumericValidation() {
        List<MovAvgModel.AbstractModelParser> parsers = new ArrayList<>(3);

        // Simple and Linear don't have any settings to test
        parsers.add(EwmaModel.PARSER);
        parsers.add(HoltWintersModel.PARSER);
        parsers.add(HoltLinearModel.PARSER);

        Object[] values = {(byte)1, 1, 1L, (short)1, (double)1};
        Map<String, Object> settings = new HashMap<>(2);

        for (MovAvgModel.AbstractModelParser parser : parsers) {
            for (Object v : values) {
                settings.put("alpha", v);

                try {
                    parser.parse(settings, "pipeline", 10);
                } catch (ParseException e) {
                    fail(parser + " parser should not have thrown SearchParseException while parsing [" +
                            v.getClass().getSimpleName() +"]");
                }
            }
        }

        for (MovAvgModel.AbstractModelParser parser : parsers) {
            settings.put("alpha", "abc");
            settings.put("beta", "abc");
            settings.put("gamma", "abc");

            try {
                parser.parse(settings, "pipeline", 10);
            } catch (ParseException e) {
                //all good
                continue;
            }

            fail(parser + " parser should have thrown SearchParseException while parsing [String]");
        }
    }
}
