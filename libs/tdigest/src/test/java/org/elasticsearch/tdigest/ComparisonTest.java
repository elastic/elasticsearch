/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class ComparisonTest {
    /**
     * This is a demo as well as a test. The scenario is that we have a thing that
     * normally has a moderately long-tailed distribution of response times. Then
     * some small fraction of transactions take 5x longer than normal. We need to
     * detect this by looking at the overall response time distribution.
     */
    @Test
    public void detectLatencyProblem() throws FileNotFoundException {
        Random gen = new Random();

        try (PrintStream out = new PrintStream("detector.csv")) {
            out.printf("name,rate,t,failure,llr\n");
            runSimulation(gen, new TdigestDetector(), out, 1000);
            runSimulation(gen, new TdigestDetector(), out, 100);
            runSimulation(gen, new TdigestDetector(), out, 10);

            runSimulation(gen, new LogHistogramDetector(), out, 1000);
            runSimulation(gen, new LogHistogramDetector(), out, 100);
            runSimulation(gen, new LogHistogramDetector(), out, 10);
        }
    }

    private void runSimulation(Random gen, Detector d, PrintStream out, double rate) {
        double dt = 1 / rate;

        double t = 0;
        double currentMinute = 0;

        // compare the distribution each minute against the previous hour
        double failureRate = 0;
        while (t < 2 * 7200) {
            if (t - currentMinute >= 60) {
                currentMinute += 60;
                if (d.isReady()) {
                    out.printf("%s, %.0f, %.0f, %.0f, %.3f\n",
                            d.name(), rate, currentMinute, failureRate > 0 ? -Math.log10(failureRate) : 0, d.score());
                }
                d.flush();
            }

            if (t >= 7200) {
                // after one hour of no failure, we add 0.1% failures, half an hour later we go to 1% failure rate
                if (t >= 7200 + 3600) {
                    failureRate = 0.01;
                } else {
                    failureRate = 0.001;
                }
            } else {
                failureRate = 0;
            }

            d.add(latencySampler(failureRate, gen));
            t += -dt * Math.log(gen.nextDouble());
        }
    }

    private interface Detector {
        boolean isReady();
        void add(double sample);
        void flush();
        double score();
        String name();
    }

    private static class TdigestDetector implements Detector {
        double[] cuts = new double[]{0.9, 0.99, 0.999, 0.9999};

        List<TDigest> history = new ArrayList<>();
        TDigest current = new MergingDigest(100);

        @Override
        public boolean isReady() {
            return history.size() >= 60;
        }

        @Override
        public void add(double sample) {
            current.add(sample);
        }

        @Override
        public void flush() {
            history.add(current);
            current = new MergingDigest(100);
        }

        @Override
        public double score() {
            TDigest ref = new MergingDigest(100);
            ref.add(history.subList(history.size() - 60, history.size()));
            return Comparison.compareChi2(ref, current, cuts);
        }

        @Override
        public String name() {
            return "t-digest";
        }
    }

    private static class LogHistogramDetector implements Detector {
        List<Histogram> history = new ArrayList<>();
        LogHistogram current = new LogHistogram(0.1e-3, 1);

        @Override
        public boolean isReady() {
            return history.size() >= 60;
        }

        @Override
        public void add(double sample) {
            current.add(sample);
        }

        @Override
        public void flush() {
            history.add(current);
            current = new LogHistogram(0.1e-3, 1);
        }

        @Override
        public double score() {
            Histogram ref = new LogHistogram(0.1e-3, 1);
            ref.add(history);
            return Comparison.compareChi2(ref, current);
        }

        @Override
        public String name() {
            return "log-histogram";
        }
    }

    private double latencySampler(double failed, Random gen) {
        if (gen.nextDouble() < failed) {
            return 50e-3 * Math.exp(gen.nextGaussian() / 2);
        } else {
            return 10e-3 * Math.exp(gen.nextGaussian() / 2);
        }
    }

    @Test
    public void compareMergingDigests() {
        TDigest d1 = new MergingDigest(100);
        TDigest d2 = new MergingDigest(100);

        d1.add(1);
        d2.add(3);
        assertEquals(2.77, Comparison.compareChi2(d1, d2, new double[]{1}), 0.01);

        Random r = new Random();
        int failed = 0;
        for (int i = 0; i < 1000; i++) {
            d1 = new MergingDigest(100);
            d2 = new MergingDigest(100);
            MergingDigest d3 = new MergingDigest(100);
            for (int j = 0; j < 10000; j++) {
                // these should look the same
                d1.add(r.nextGaussian());
                d2.add(r.nextGaussian());
                // can we see a small difference
                d3.add(r.nextGaussian() + 0.3);
            }

            // 5 degrees of freedom, Pr(llr > 20) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[]{0.1, 0.3, 0.5, 0.8, 0.9}) > 25) {
                failed++;
            }

            // 1 degree of freedom, Pr(llr > 10) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[]{0.1}) > 20) {
                failed++;
            }

            // 1 degree of freedom, Pr(llr > 10) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[]{0.5}) > 20) {
                failed++;
            }

            if (Comparison.compareChi2(d1, d3, new double[]{0.1, 0.5, 0.9}) < 90) {
                failed++;
            }
        }
        assertEquals(0, failed, 5);
        System.out.printf("Failed %d times (up to 5 acceptable)", failed);
    }

    @Test
    public void ks() {
        Random r = new Random();
        double mean = 0;
        double s2 = 0;
        for (int i = 0; i < 10; i++) {
            MergingDigest d1 = new MergingDigest(100);
            MergingDigest d2 = new MergingDigest(100);
            MergingDigest d3 = new MergingDigest(100);
            for (int j = 0; j < 1000000; j++) {
                d1.add(r.nextGaussian());
                d2.add(r.nextGaussian() + 1);
                d3.add(r.nextGaussian());
            }
            double ks = Comparison.ks(d1, d2);
            // this value is slightly lower than it should be (by about 0.9)
            assertEquals(269.5, ks, 3);
            double newMean = mean + (ks - mean) / (i + 1);
            s2 += (ks - mean) * (ks - newMean);
            mean = newMean;

            assertEquals(0, Comparison.ks(d1, d3), 3.5);
        }

        System.out.printf("%.5f %.5f\n", mean, Math.sqrt(s2 / 10));
    }

    @Test
    public void compareLogHistograms() {
        Random r = new Random();
        int failed = 0;

        try {
            Comparison.compareChi2(new LogHistogram(10e-6, 10), new LogHistogram(1e-6, 1));
            fail("Should have detected incompatible histograms (lower bound)");
        } catch (IllegalArgumentException e) {
            assertEquals("Incompatible histograms in terms of size or bounds", e.getMessage());
        }
        try {
            Comparison.compareChi2(new LogHistogram(10e-6, 10), new LogHistogram(10e-6, 1));
            fail("Should have detected incompatible histograms (size)");
        } catch (IllegalArgumentException e) {
            assertEquals("Incompatible histograms in terms of size or bounds", e.getMessage());
        }

        for (int i = 0; i < 1000; i++) {
            LogHistogram d1 = new LogHistogram(10e-6, 10);
            LogHistogram d2 = new LogHistogram(10e-6, 10);
            LogHistogram d3 = new LogHistogram(10e-6, 10);
            for (int j = 0; j < 10000; j++) {
                // these should look the same
                d1.add(Math.exp(r.nextGaussian()));
                d2.add(Math.exp(r.nextGaussian()));
                // can we see a small difference
                d3.add(Math.exp(r.nextGaussian() + 0.5));
            }

            // 144 degrees of freedom, Pr(llr > 250) < 1e-6
            if (Comparison.compareChi2(d1, d2) > 250) {
                failed++;
            }

            if (Comparison.compareChi2(d1, d3) < 1000) {
                failed++;
            }
        }
        assertEquals(0, failed, 5);
    }

    @Test
    public void llr() {
        double[][] count = new double[2][2];
        count[0][0] = 1;
        count[1][1] = 1;
        assertEquals(2.77, Comparison.llr(count), 0.01);

        count[0][0] = 3;
        count[0][1] = 1;
        count[1][0] = 1;
        count[1][1] = 3;
        assertEquals(2.09, Comparison.llr(count), 0.01);

        count[1][1] = 5;
        assertEquals(3.55, Comparison.llr(count), 0.01);
    }
}
