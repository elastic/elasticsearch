/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests scaling properties of t-digest variants
 */
public class ScaleTests extends ESTestCase {

    public void testGrowth() {
        for (Limit limit : new Limit[] {
            new RootLinearLimit(),
            new RootLimit(),
            new StandardLimit(),
            new LinearLimit(),
            new PiecewiseLinearLimit(0.05),
            new PiecewiseLinearLimit(0.1),
            new PiecewiseLinearLimit(0.2), }) {
            for (long n : new long[] { 1000, 10000, 100000, 1000000L, 10000000L, 100000000L, 1000000000L }) {
                List<Centroid> r = size(n, 200.0, limit);
                int nonTrivial = 0;
                for (Centroid centroid : r) {
                    if (centroid.count > 1) {
                        nonTrivial++;
                    }
                }
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    public List<Centroid> size(long n, @SuppressWarnings("SameParameterValue") double compression, Limit limit) {
        if (compression <= 0) {
            compression = 50;
        }

        if (limit == null) {
            limit = new StandardLimit();
        }

        double total = 0;
        long i = 0;
        List<Centroid> r = new ArrayList<>();
        while (i < n) {
            double mean = i;
            int count = 1;
            i++;
            double qx = total / n;

            while (i < n && count + 1 <= Math.max(1, limit.limit(n, qx) / compression)) {
                count++;
                mean += (i - mean) / count;
                qx = (total + count / 2) / n;
                i++;
            }
            total += count;
            r.add(new Centroid(mean, count));
        }
        return r;
    }

    public static class Centroid {
        final double mean;
        final int count;

        @SuppressWarnings("WeakerAccess")
        public Centroid(double mean, int count) {
            this.mean = mean;
            this.count = count;
        }
    }

    public interface Limit {
        double limit(long n, double q);
    }

    public static class StandardLimit implements Limit {
        @Override
        public double limit(long n, double q) {
            return 4 * n * q * (1 - q);
        }
    }

    public static class RootLimit implements Limit {
        @Override
        public double limit(long n, double q) {
            return 2 * n * Math.sqrt(q * (1 - q));
        }
    }

    public static class LinearLimit implements Limit {
        @Override
        public double limit(long n, double q) {
            return 2 * n * Math.min(q, 1 - q);
        }
    }

    public static class RootLinearLimit implements Limit {
        @Override
        public double limit(long n, double q) {
            return n * Math.sqrt(2 * Math.min(q, 1 - q));
        }
    }

    public static class PowerLinearLimit implements Limit {
        private final double exp;

        public PowerLinearLimit(double exp) {
            this.exp = exp;
        }

        @Override
        public double limit(long n, double q) {
            return n * Math.pow(2 * Math.min(q, 1 - q), exp);
        }
    }

    private class PiecewiseLinearLimit implements Limit {
        private final double cut;

        PiecewiseLinearLimit(double cut) {
            this.cut = cut;
        }

        @Override
        public double limit(long n, double q) {
            if (q < cut) {
                return n * q / cut;
            } else if (1 - q < cut) {
                return limit(n, 1 - q);
            } else {
                return n;
            }

        }
    }
}
