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

package org.elasticsearch.search.aggregations.metrics;

import com.google.common.collect.Lists;
import org.apache.mahout.math.jet.random.AbstractContinousDistribution;
import org.apache.mahout.math.jet.random.Gamma;
import org.apache.mahout.math.jet.random.Normal;
import org.apache.mahout.math.jet.random.Uniform;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.hamcrest.Matchers.lessThan;

// imported tests from upstream's TDigestTest
public class TDigestStateTests extends ElasticsearchTestCase {

    @Test
    public void testUniform() {
        Random gen = getRandom();
        for (int i = 0; i < 10; i++) {
            runTest(new Uniform(0, 1, gen), 100,
                    new double[]{0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999},
                    "uniform");
        }
    }

    @Test
    public void testGamma() {
        // this Gamma distribution is very heavily skewed.  The 0.1%-ile is 6.07e-30 while
        // the median is 0.006 and the 99.9th %-ile is 33.6 while the mean is 1.
        // this severe skew means that we have to have positional accuracy that
        // varies by over 11 orders of magnitude.
        Random gen = getRandom();
        for (int i = 0; i < 10; i++) {
            runTest(new Gamma(0.1, 0.1, gen), 100,
//                    new double[]{6.0730483624079e-30, 6.0730483624079e-20, 6.0730483627432e-10, 5.9339110446023e-03,
//                            2.6615455373884e+00, 1.5884778179295e+01, 3.3636770117188e+01},
                    new double[]{0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999},
                    "gamma");
        }
    }

    @Test
    public void testNarrowNormal() {
        // this mixture of a uniform and normal distribution has a very narrow peak which is centered
        // near the median.  Our system should be scale invariant and work well regardless.
        final Random gen = getRandom();
        AbstractContinousDistribution mix = new AbstractContinousDistribution() {
            AbstractContinousDistribution normal = new Normal(0, 1e-5, gen);
            AbstractContinousDistribution uniform = new Uniform(-1, 1, gen);

            @Override
            public double nextDouble() {
                double x;
                if (gen.nextDouble() < 0.5) {
                    x = uniform.nextDouble();
                } else {
                    x = normal.nextDouble();
                }
                return x;
            }
        };

        for (int i = 0; i < 10; i++) {
            runTest(mix, 100, new double[]{0.001, 0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99, 0.999}, "mixture");
        }
    }

    @Test
    public void testRepeatedValues() {
        final Random gen = getRandom();

        // 5% of samples will be 0 or 1.0.  10% for each of the values 0.1 through 0.9
        AbstractContinousDistribution mix = new AbstractContinousDistribution() {
            @Override
            public double nextDouble() {
                return Math.rint(gen.nextDouble() * 10) / 10.0;
            }
        };

        TDigestState dist = new TDigestState((double) 1000);
        List<Double> data = Lists.newArrayList();
        for (int i1 = 0; i1 < 100000; i1++) {
            double x = mix.nextDouble();
            data.add(x);
            dist.add(x);
        }

        // I would be happier with 5x compression, but repeated values make things kind of weird
        assertTrue("Summary is too large", dist.centroidCount() < 10 * (double) 1000);

        // all quantiles should round to nearest actual value
        for (int i = 0; i < 10; i++) {
            double z = i / 10.0;
            // we skip over troublesome points that are nearly halfway between
            for (double delta : new double[] {0.01, 0.02, 0.03, 0.07, 0.08, 0.09}) {
                double q = z + delta;
                double cdf = dist.cdf(q);
                // we also relax the tolerances for repeated values
                assertEquals(String.format(Locale.ROOT, "z=%.1f, q = %.3f, cdf = %.3f", z, q, cdf), z + 0.05, cdf, 0.01);

                double estimate = dist.quantile(q);
                assertEquals(String.format(Locale.ROOT, "z=%.1f, q = %.3f, cdf = %.3f, estimate = %.3f", z, q, cdf, estimate), Math.rint(q * 10) / 10.0, estimate, 0.001);
            }
        }
    }

    @Test
    public void testSequentialPoints() {
        for (int i = 0; i < 10; i++) {
            runTest(new AbstractContinousDistribution() {
                double base = 0;

                @Override
                public double nextDouble() {
                    base += Math.PI * 1e-5;
                    return base;
                }
            }, 100, new double[]{0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999},
                    "sequential");
        }
    }

    public void runTest(AbstractContinousDistribution gen, double sizeGuide, double[] qValues, String tag) {
        final int len = 100000;
        final TDigestState dist = new TDigestState(sizeGuide);
        double[] data = new double[len];
        for (int i = 0; i < len; ++i) {
            double x = gen.nextDouble();
            data[i] = x;
            dist.add(x);
        }
        dist.compress();
        Arrays.sort(data);

        double[] xValues = qValues.clone();
        for (int i = 0; i < qValues.length; i++) {
            double ix = data.length * qValues[i] - 0.5;
            int index = (int) Math.floor(ix);
            double p = ix - index;
            xValues[i] = data[index] * (1 - p) + data[index + 1] * p;
        }

        assertThat("Summary is too large", (double) dist.centroidCount(), lessThan(sizeGuide * 10));
        int softErrors = 0;
        for (int i = 0; i < xValues.length; i++) {
            double x = xValues[i];
            double q = qValues[i];
            double estimate = dist.cdf(x);
            assertEquals(q, estimate, 0.005);

            estimate = cdf(dist.quantile(q), data);
            if (Math.abs(q - estimate) > 0.005) {
                softErrors++;
            }
            assertEquals(q, estimate, 0.012);
        }
        assertTrue(softErrors < 3);
    }

    private double cdf(final double x, double[] data) {
        int n1 = 0;
        int n2 = 0;
        for (double v : data) {
            n1 += (v < x) ? 1 : 0;
            n2 += (v <= x) ? 1 : 0;
        }
        return (n1 + n2) / 2.0 / data.length;
    }
}
