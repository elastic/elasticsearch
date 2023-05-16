/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assume.assumeTrue;

/**
 * Tests scaling properties of t-digest variants
 */
public class ScaleTest {
    @Test
    public void testGrowth() {
        assumeTrue(Boolean.parseBoolean(System.getProperty("runSlowTests")));
        for (Limit limit : new Limit[]{
                new RootLinearLimit(), new RootLimit(),
                new StandardLimit(), new LinearLimit(), new PiecewiseLinearLimit(0.05),
                new PiecewiseLinearLimit(0.1), new PiecewiseLinearLimit(0.2),
        }) {
            for (long n : new long[]{1000, 10000, 100000, 1000000L, 10000000L, 100000000L, 1000000000L}) {
                List<Centroid> r = size(n, 200.0, limit);
                int nonTrivial = 0;
                for (Centroid centroid : r) {
                    if (centroid.count > 1) {
                        nonTrivial++;
                    }
                }
                System.out.printf("%s\t%d\t%d\t%d\n", limit.getClass().getSimpleName(), n, r.size(), nonTrivial);
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
