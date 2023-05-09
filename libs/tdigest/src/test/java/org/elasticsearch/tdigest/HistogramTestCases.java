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

import java.io.*;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class HistogramTestCases {
    boolean useLinearBuckets;
    HistogramFactory factory;

    @Test
    public void testEmpty() {
        Histogram x = factory.create(1, 100);
        double[] bins = x.getBounds();
        assertEquals(1, bins[0], 1e-5);
        assertTrue(x.lowerBound(bins.length) >= 100.0);
        assertTrue(bins.length >= 90);
    }

    void doLinear(double idealChi2, double chi2SD, int binCount) throws FileNotFoundException {
        int n = 10000;
        int trials = 1000;
        // this should be about 160 by theory since that is where the cuts go above 1, but
        // there is some small residual issue that causes it to be a bit bigger.
        // these values are empirical against the current implementation. The fact that many counts are small may
        // be the reason for the slight deviation.

        double[] min = new double[binCount];
        double[] max = new double[binCount];

        // 165.4, 18, 212
        // 157, 18, 201


        Arrays.fill(min, 1);
        int n1above = 0;
        int n2above = 0;
        int n1below = 0;
        int n2below = 0;
        try (PrintWriter out = new PrintWriter("data.csv")) {
            out.print("j,cut,low,high,k,expected,err\n");
            double mean = 0;
            double sd = 0;
            for (int j = 0; j < trials; j++) {
                Histogram x = factory.create(1e-3, 10);
                long[] counts = x.getCounts();
                double[] cuts = x.getBounds();
                assertTrue(min.length >= cuts.length);
                assertTrue(max.length >= cuts.length);

                Random rand = new Random();
                for (int i = 0; i < n; i++) {
                    double u = rand.nextDouble();
                    x.add(u);
                    int k = x.bucket(u);
                    min[k] = Math.min(min[k], u);
                    max[k] = Math.max(max[k], u);
                }
                double sum = 0;
                double maxErr = 0;
                int i = 0;
                while (i < cuts.length - 1 && cuts[i] < 1.1) {
                    double lowBound = Math.min(1, cuts[i]);
                    if (i == 0) {
                        lowBound = 0;
                    }
                    double highBound = Math.min(1, cuts[i + 1]);
                    double expected = n * (highBound - lowBound);


                    double err = 0;
                    if (counts[i] > 0) {
                        err = counts[i] * Math.log(counts[i] / expected);
                        sum += err;

                        if (i > 0) {
                            assertTrue(String.format("%d: %.5f > %.5f", i, cuts[i], min[i]), cuts[i] <= min[i]);
                        }
                        assertTrue(String.format("%d: %.5f > %.5f", i, max[i], cuts[i + 1]), cuts[i + 1] >= max[i]);
                    }
                    out.printf("%d,%.4f,%.4f,%.4f,%d,%.4f,%.1f\n", j, cuts[i], lowBound, highBound, counts[i], expected, err);
                    maxErr = Math.max(maxErr, err);
                    i++;
                }
                while (i < cuts.length) {
                    assertEquals(0, counts[i]);
                    i++;
                }
                sum = 2 * sum;
                if (sum > idealChi2 + 3 * chi2SD) {
                    n2above++;
                }
                if (sum > idealChi2 + 2 * chi2SD) {
                    n1above++;
                }
                if (sum < idealChi2 - 3 * chi2SD) {
                    n2below++;
                }
                if (sum < idealChi2 - 2 * chi2SD) {
                    n1below++;
                }
                double old = mean;
                mean += (sum - mean) / (j + 1);
                sd += (sum - mean) * (sum - old);
            }
            System.out.printf("Checking χ^2 = %.4f ± %.1f against expected %.4f ± %.1f\n",
                    mean, Math.sqrt(sd / trials), idealChi2, chi2SD);
            // verify that the chi^2 score for counts is as expected
            assertEquals("χ^2 > expect + 2*sd too often", 0, n1above, 0.05 * trials);
            // 3*sigma above for a chi^2 distribution happens more than you might think
            assertEquals("χ^2 > expect + 3*sd too often", 0, n2above, 0.01 * trials);
            // the bottom side of the chi^2 distribution is a bit tighter
            assertEquals("χ^2 < expect - 2*sd too often", 0, n1below, 0.03 * trials);
            assertEquals("χ^2 < expect - 3*sd too often", 0, n2below, 0.06 * trials);
        }
    }

    /**
     * The point of this test is to make sure that the floating point representation
     * can be used as a quick approximation of log_2
     *
     * @throws FileNotFoundException If we can't open an output file
     */
    @Test
    public void testFitToLog() throws FileNotFoundException {
        double scale = Math.pow(2, 52);
        double x = 0.001;
        // 4 bits, worst case is mid octave
        double lowerBound = 1 / 16.0 * Math.sqrt(2);
        try (PrintWriter out = new PrintWriter("log-fit.csv")) {
            out.printf("x,y1,y2\n");
            while (x < 10) {
                long xz = Double.doubleToLongBits(x);
                // the magic 0x3ff is the offset for the floating point exponent
                double v1 = xz / scale - 0x3ff;
                double v2 = Math.log(x) / Math.log(2);
                out.printf("%.6f,%.6f,%.6f\n", x, v1, v2);
                assertTrue(v2 - v1 > 0);
                assertTrue(v2 - v1 < lowerBound);
                x *= 1.02;
            }
        }
    }

    void testBinSizes(int baseBinIndex, int bigBinIndex, Histogram histogram) {
        assertEquals(baseBinIndex, histogram.bucket(10.01e-3));
        assertEquals(baseBinIndex, histogram.bucket(10e-3));
        assertEquals(bigBinIndex, histogram.bucket(2.235));
    }

    @Test
    public void testCompression() {
        int n = 1000000;
        Histogram x = factory.create(1e-3, 10);

        Random rand = new Random();
        for (int i = 0; i < n; i++) {
            x.add(rand.nextDouble());
        }
        long[] compressed = x.getCompressedCounts();
        assertTrue(compressed.length < 45);
        long[] uncompressed = new long[x.getCounts().length];
        long[] counts = x.getCounts();

        int k = Simple64.decompress(LongBuffer.wrap(compressed), uncompressed);
        assertEquals(k, counts.length);
        for (int i = 0; i < uncompressed.length; i++) {
            assertEquals(counts[i], uncompressed[i]);
        }
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        int n = 1000000;
        Histogram x = factory.create(1e-3, 10);

        Random rand = new Random();
        for (int i = 0; i < n; i++) {
            x.add(rand.nextDouble());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(1000);
        ObjectOutputStream xout = new ObjectOutputStream(out);
        x.writeObject(xout);
        xout.close();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray(), 0, out.size());
        Histogram y = factory.create(0.1, 10);
        y.readObject(new ObjectInputStream(in));

        assertArrayEquals(x.getBounds(), y.getBounds(), 1e-10);
        assertArrayEquals(x.getCounts(), y.getCounts());
    }

    public interface HistogramFactory {
        Histogram create(double min, double max);
    }
}
