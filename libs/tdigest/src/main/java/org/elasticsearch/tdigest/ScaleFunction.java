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

/**
 * Encodes the various scale functions for t-digests. These limits trade accuracy near the tails against accuracy near
 * the median in different ways. For instance, K_0 has uniform cluster sizes and results in constant accuracy (in terms
 * of q) while K_3 has cluster sizes proportional to min(q,1-q) which results in very much smaller error near the tails
 * and modestly increased error near the median.
 * <p>
 * The base forms (K_0, K_1, K_2 and K_3) all result in t-digests limited to a number of clusters equal to the
 * compression factor. The K_2_NO_NORM and K_3_NO_NORM versions result in the cluster count increasing roughly with
 * log(n).
 */
public enum ScaleFunction {
    /**
     * Generates uniform cluster sizes. Used for comparison only.
     */
    K_0 {
        @Override
        public double k(double q, double compression, double n) {
            return compression * q / 2;
        }

        @Override
        public double k(double q, double normalizer) {
            return normalizer * q;
        }

        @Override
        public double q(double k, double compression, double n) {
            return 2 * k / compression;
        }

        @Override
        public double q(double k, double normalizer) {
            return k / normalizer;
        }

        @Override
        public double max(double q, double compression, double n) {
            return 2 / compression;
        }

        @Override
        public double max(double q, double normalizer) {
            return 1 / normalizer;
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression / 2;
        }
    },

    /**
     * Generates cluster sizes proportional to sqrt(q*(1-q)). This gives constant relative accuracy if accuracy is
     * proportional to squared cluster size. It is expected that K_2 and K_3 will give better practical results.
     */
    K_1 {
        @Override
        public double k(final double q, final double compression, double n) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return compression * Math.asin(2 * q - 1) / (2 * Math.PI);
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double k(final double q, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return normalizer * Math.asin(2 * q - 1);
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }


        @Override
        public double q(double k, final double compression, double n) {
            Function f = new Function() {
                @Override
                double apply(double k) {
                    return (Math.sin(k * (2 * Math.PI / compression)) + 1) / 2;
                }
            };
            return ScaleFunction.limitCall(f, k, -compression / 4, compression / 4);
        }

        @Override
        public double q(double k, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double x) {
                    return (Math.sin(x) + 1) / 2;
                }
            };
            double x = k / normalizer;
            return ScaleFunction.limitCall(f, x, -Math.PI / 2, Math.PI / 2);
        }

        @Override
        public double max(double q, double compression, double n) {
            if (q <= 0) {
                return 0;
            } else if (q >= 1) {
                return 0;
            } else {
                return 2 * Math.sin(Math.PI / compression) * Math.sqrt(q * (1 - q));
            }
        }

        @Override
        public double max(double q, double normalizer) {
            if (q <= 0) {
                return 0;
            } else if (q >= 1) {
                return 0;
            } else {
                return 2 * Math.sin(0.5 / normalizer) * Math.sqrt(q * (1 - q));
            }
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression / (2 * Math.PI);
        }
    },

    /**
     * Generates cluster sizes proportional to sqrt(q*(1-q)) but avoids computation of asin in the critical path by
     * using an approximate version.
     */
    K_1_FAST {
        @Override
        public double k(double q, final double compression, double n) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return compression * fastAsin(2 * q - 1) / (2 * Math.PI);
                }
            };
            return ScaleFunction.limitCall(f, q, 0, 1);
        }

        @Override
        public double k(double q, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return normalizer * fastAsin(2 * q - 1);
                }
            };
            return ScaleFunction.limitCall(f, q, 0, 1);
        }

        @Override
        public double q(double k, double compression, double n) {
            return (Math.sin(k * (2 * Math.PI / compression)) + 1) / 2;
        }

        @Override
        public double q(double k, double normalizer) {
            return (Math.sin(k / normalizer) + 1) / 2;
        }

        @Override
        public double max(double q, double compression, double n) {
            if (q <= 0) {
                return 0;
            } else if (q >= 1) {
                return 0;
            } else {
                return 2 * Math.sin(Math.PI / compression) * Math.sqrt(q * (1 - q));
            }
        }

        @Override
        public double max(double q, double normalizer) {
            if (q <= 0) {
                return 0;
            } else if (q >= 1) {
                return 0;
            } else {
                return 2 * Math.sin(0.5 / normalizer) * Math.sqrt(q * (1 - q));
            }
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression / (2 * Math.PI);
        }
    },

    /**
     * Generates cluster sizes proportional to q*(1-q). This makes tail error bounds tighter than for K_1. The use of a
     * normalizing function results in a strictly bounded number of clusters no matter how many samples.
     */
    K_2 {
        @Override
        public double k(double q, final double compression, final double n) {
            if (n <= 1) {
                if (q <= 0) {
                    return -10;
                } else if (q >= 1) {
                    return 10;
                } else {
                    return 0;
                }
            }
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return compression * Math.log(q / (1 - q)) / Z(compression, n);
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double k(double q, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return Math.log(q / (1 - q)) * normalizer;
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double q(double k, double compression, double n) {
            double w = Math.exp(k * Z(compression, n) / compression);
            return w / (1 + w);
        }

        @Override
        public double q(double k, double normalizer) {
            double w = Math.exp(k / normalizer);
            return w / (1 + w);
        }

        @Override
        public double max(double q, double compression, double n) {
            return Z(compression, n) * q * (1 - q) / compression;
        }

        @Override
        public double max(double q, double normalizer) {
            return q * (1 - q) / normalizer;
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression / Z(compression, n);
        }

        private double Z(double compression, double n) {
            return 4 * Math.log(n / compression) + 24;
        }
    },

    /**
     * Generates cluster sizes proportional to min(q, 1-q). This makes tail error bounds tighter than for K_1 or K_2.
     * The use of a normalizing function results in a strictly bounded number of clusters no matter how many samples.
     */
    K_3 {
        @Override
        public double k(double q, final double compression, final double n) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    if (q <= 0.5) {
                        return compression * Math.log(2 * q) / Z(compression, n);
                    } else {
                        return -k(1 - q, compression, n);
                    }
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double k(double q, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    if (q <= 0.5) {
                        return Math.log(2 * q) * normalizer;
                    } else {
                        return -k(1 - q, normalizer);
                    }
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double q(double k, double compression, double n) {
            if (k <= 0) {
                return Math.exp(k * Z(compression, n) / compression) / 2;
            } else {
                return 1 - q(-k, compression, n);
            }
        }

        @Override
        public double q(double k, double normalizer) {
            if (k <= 0) {
                return Math.exp(k / normalizer) / 2;
            } else {
                return 1 - q(-k, normalizer);
            }
        }

        @Override
        public double max(double q, double compression, double n) {
            return Z(compression, n) * Math.min(q, 1 - q) / compression;
        }

        @Override
        public double max(double q, double normalizer) {
            return Math.min(q, 1 - q) / normalizer;
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression / Z(compression, n);
        }

        private double Z(double compression, double n) {
            return 4 * Math.log(n / compression) + 21;
        }
    },

    /**
     * Generates cluster sizes proportional to q*(1-q). This makes the tail error bounds tighter. This version does not
     * use a normalizer function and thus the number of clusters increases roughly proportional to log(n). That is good
     * for accuracy, but bad for size and bad for the statically allocated MergingDigest, but can be useful for
     * tree-based implementations.
     */
    K_2_NO_NORM {
        @Override
        public double k(double q, final double compression, double n) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return compression * Math.log(q / (1 - q));
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double k(double q, final double normalizer) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    return normalizer * Math.log(q / (1 - q));
                }
            };
            return ScaleFunction.limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double q(double k, double compression, double n) {
            double w = Math.exp(k / compression);
            return w / (1 + w);
        }

        @Override
        public double q(double k, double normalizer) {
            double w = Math.exp(k / normalizer);
            return w / (1 + w);
        }

        @Override
        public double max(double q, double compression, double n) {
            return q * (1 - q) / compression;
        }

        @Override
        public double max(double q, double normalizer) {
            return q * (1 - q) / normalizer;
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression;
        }
    },

    /**
     * Generates cluster sizes proportional to min(q, 1-q). This makes the tail error bounds tighter. This version does
     * not use a normalizer function and thus the number of clusters increases roughly proportional to log(n). That is
     * good for accuracy, but bad for size and bad for the statically allocated MergingDigest, but can be useful for
     * tree-based implementations.
     */
    K_3_NO_NORM {
        @Override
        public double k(double q, final double compression, final double n) {
            Function f = new Function() {
                @Override
                double apply(double q) {
                    if (q <= 0.5) {
                        return compression * Math.log(2 * q);
                    } else {
                        return -k(1 - q, compression, n);
                    }
                }
            };
            return limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double k(double q, final double normalizer) {
            // poor man's lambda, sigh
            Function f = new Function() {
                @Override
                double apply(double q) {
                    if (q <= 0.5) {
                        return normalizer * Math.log(2 * q);
                    } else {
                        return -k(1 - q, normalizer);
                    }
                }
            };
            return limitCall(f, q, 1e-15, 1 - 1e-15);
        }

        @Override
        public double q(double k, double compression, double n) {
            if (k <= 0) {
                return Math.exp(k / compression) / 2;
            } else {
                return 1 - q(-k, compression, n);
            }
        }

        @Override
        public double q(double k, double normalizer) {
            if (k <= 0) {
                return Math.exp(k / normalizer) / 2;
            } else {
                return 1 - q(-k, normalizer);
            }
        }

        @Override
        public double max(double q, double compression, double n) {
            return Math.min(q, 1 - q) / compression;
        }

        @Override
        public double max(double q, double normalizer) {
            return Math.min(q, 1 - q) / normalizer;
        }

        @Override
        public double normalizer(double compression, double n) {
            return compression;
        }
    };                    // max weight is min(q,1-q), should improve tail accuracy even more

    /**
     * Converts a quantile to the k-scale. The total number of points is also provided so that a normalizing function
     * can be computed if necessary.
     *
     * @param q           The quantile
     * @param compression Also known as delta in literature on the t-digest
     * @param n           The total number of samples
     * @return The corresponding value of k
     */
    abstract public double k(double q, double compression, double n);

    /**
     * Converts  a quantile to the k-scale. The normalizer value depends on compression and (possibly) number of points
     * in the digest. #normalizer(double, double)
     *
     * @param q          The quantile
     * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
     *                   digest.
     * @return The corresponding value of k
     */
    abstract public double k(double q, double normalizer);

    /**
     * Computes q as a function of k. This is often faster than finding k as a function of q for some scales.
     *
     * @param k           The index value to convert into q scale.
     * @param compression The compression factor (often written as &delta;)
     * @param n           The number of samples already in the digest.
     * @return The value of q that corresponds to k
     */
    abstract public double q(double k, double compression, double n);

    /**
     * Computes q as a function of k. This is often faster than finding k as a function of q for some scales.
     *
     * @param k          The index value to convert into q scale.
     * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
     *                   digest.
     * @return The value of q that corresponds to k
     */
    abstract public double q(double k, double normalizer);

    /**
     * Computes the maximum relative size a cluster can have at quantile q. Note that exactly where within the range
     * spanned by a cluster that q should be isn't clear. That means that this function usually has to be taken at
     * multiple points and the smallest value used.
     * <p>
     * Note that this is the relative size of a cluster. To get the max number of samples in the cluster, multiply this
     * value times the total number of samples in the digest.
     *
     * @param q           The quantile
     * @param compression The compression factor, typically delta in the literature
     * @param n           The number of samples seen so far in the digest
     * @return The maximum number of samples that can be in the cluster
     */
    abstract public double max(double q, double compression, double n);

    /**
     * Computes the maximum relative size a cluster can have at quantile q. Note that exactly where within the range
     * spanned by a cluster that q should be isn't clear. That means that this function usually has to be taken at
     * multiple points and the smallest value used.
     * <p>
     * Note that this is the relative size of a cluster. To get the max number of samples in the cluster, multiply this
     * value times the total number of samples in the digest.
     *
     * @param q          The quantile
     * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
     *                   digest.
     * @return The maximum number of samples that can be in the cluster
     */
    abstract public double max(double q, double normalizer);

    /**
     * Computes the normalizer given compression and number of points.
     * @param compression The compression parameter for the digest
     * @param n The number of samples seen so far
     * @return The normalizing factor for the scale function
     */
    abstract public double normalizer(double compression, double n);

    /**
     * Approximates asin to within about 1e-6. This approximation works by breaking the range from 0 to 1 into 5 regions
     * for all but the region nearest 1, rational polynomial models get us a very good approximation of asin and by
     * interpolating as we move from region to region, we can guarantee continuity and we happen to get monotonicity as
     * well.  for the values near 1, we just use Math.asin as our region "approximation".
     *
     * @param x sin(theta)
     * @return theta
     */
    static double fastAsin(double x) {
        if (x < 0) {
            return -fastAsin(-x);
        } else if (x > 1) {
            return Double.NaN;
        } else {
            // Cutoffs for models. Note that the ranges overlap. In the
            // overlap we do linear interpolation to guarantee the overall
            // result is "nice"
            double c0High = 0.1;
            double c1High = 0.55;
            double c2Low = 0.5;
            double c2High = 0.8;
            double c3Low = 0.75;
            double c3High = 0.9;
            double c4Low = 0.87;
            if (x > c3High) {
                return Math.asin(x);
            } else {
                // the models
                double[] m0 = {0.2955302411, 1.2221903614, 0.1488583743, 0.2422015816, -0.3688700895, 0.0733398445};
                double[] m1 = {-0.0430991920, 0.9594035750, -0.0362312299, 0.1204623351, 0.0457029620, -0.0026025285};
                double[] m2 = {-0.034873933724, 1.054796752703, -0.194127063385, 0.283963735636, 0.023800124916, -0.000872727381};
                double[] m3 = {-0.37588391875, 2.61991859025, -2.48835406886, 1.48605387425, 0.00857627492, -0.00015802871};

                // the parameters for all of the models
                double[] vars = {1, x, x * x, x * x * x, 1 / (1 - x), 1 / (1 - x) / (1 - x)};

                // raw grist for interpolation coefficients
                double x0 = bound((c0High - x) / c0High);
                double x1 = bound((c1High - x) / (c1High - c2Low));
                double x2 = bound((c2High - x) / (c2High - c3Low));
                double x3 = bound((c3High - x) / (c3High - c4Low));

                // interpolation coefficients
                //noinspection UnnecessaryLocalVariable
                double mix0 = x0;
                double mix1 = (1 - x0) * x1;
                double mix2 = (1 - x1) * x2;
                double mix3 = (1 - x2) * x3;
                double mix4 = 1 - x3;

                // now mix all the results together, avoiding extra evaluations
                double r = 0;
                if (mix0 > 0) {
                    r += mix0 * eval(m0, vars);
                }
                if (mix1 > 0) {
                    r += mix1 * eval(m1, vars);
                }
                if (mix2 > 0) {
                    r += mix2 * eval(m2, vars);
                }
                if (mix3 > 0) {
                    r += mix3 * eval(m3, vars);
                }
                if (mix4 > 0) {
                    // model 4 is just the real deal
                    r += mix4 * Math.asin(x);
                }
                return r;
            }
        }
    }

    static abstract class Function {
        abstract double apply(double x);
    }

    static double limitCall(Function f, double x, double low, double high) {
        if (x < low) {
            return f.apply(low);
        } else if (x > high) {
            return f.apply(high);
        } else {
            return f.apply(x);
        }
    }

    private static double eval(double[] model, double[] vars) {
        double r = 0;
        for (int i = 0; i < model.length; i++) {
            r += model[i] * vars[i];
        }
        return r;
    }

    private static double bound(double v) {
        if (v <= 0) {
            return 0;
        } else if (v >= 1) {
            return 1;
        } else {
            return v;
        }
    }
}
