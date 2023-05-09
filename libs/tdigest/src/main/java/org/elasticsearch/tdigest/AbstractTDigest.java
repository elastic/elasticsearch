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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTDigest extends TDigest {
    boolean recordAllData = false;

    /**
     * Same as {@link #weightedAverageSorted(double, double, double, double)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
     */
    static double weightedAverage(double x1, double w1, double x2, double w2) {
        if (x1 <= x2) {
            return weightedAverageSorted(x1, w1, x2, w2);
        } else {
            return weightedAverageSorted(x2, w2, x1, w1);
        }
    }

    /**
     * Compute the weighted average between <code>x1</code> with a weight of
     * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
     * This expects <code>x1</code> to be less than or equal to <code>x2</code>
     * and is guaranteed to return a number in <code>[x1, x2]</code>. An
     * explicit check is required since this isn't guaranteed with floating-point
     * numbers.
     */
    private static double weightedAverageSorted(double x1, double w1, double x2, double w2) {
        assert x1 <= x2;
        final double x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return Math.max(x1, Math.min(x, x2));
    }

    static double interpolate(double x, double x0, double x1) {
        return (x - x0) / (x1 - x0);
    }

    static void encode(ByteBuffer buf, int n) {
        int k = 0;
        while (n < 0 || n > 0x7f) {
            byte b = (byte) (0x80 | (0x7f & n));
            buf.put(b);
            n = n >>> 7;
            k++;
            if (k >= 6) {
                throw new IllegalStateException("Size is implausibly large");
            }
        }
        buf.put((byte) n);
    }

    static int decode(ByteBuffer buf) {
        int v = buf.get();
        int z = 0x7f & v;
        int shift = 7;
        while ((v & 0x80) != 0) {
            if (shift > 28) {
                throw new IllegalStateException("Shift too large in decode");
            }
            v = buf.get();
            z += (v & 0x7f) << shift;
            shift += 7;
        }
        return z;
    }

    abstract void add(double x, int w, Centroid base);

    /**
     * Computes an interpolated value of a quantile that is between two centroids.
     *
     * Index is the quantile desired multiplied by the total number of samples - 1.
     *
     * @param index              Denormalized quantile desired
     * @param previousIndex      The denormalized quantile corresponding to the center of the previous centroid.
     * @param nextIndex          The denormalized quantile corresponding to the center of the following centroid.
     * @param previousMean       The mean of the previous centroid.
     * @param nextMean           The mean of the following centroid.
     * @return  The interpolated mean.
     */
    static double quantile(double index, double previousIndex, double nextIndex, double previousMean, double nextMean) {
        final double delta = nextIndex - previousIndex;
        final double previousWeight = (nextIndex - index) / delta;
        final double nextWeight = (index - previousIndex) / delta;
        return previousMean * previousWeight + nextMean * nextWeight;
    }

    /**
     * Sets up so that all centroids will record all data assigned to them.  For testing only, really.
     */
    @Override
    public TDigest recordAllData() {
        recordAllData = true;
        return this;
    }

    @Override
    public boolean isRecording() {
        return recordAllData;
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     */
    @Override
    public void add(double x) {
        add(x, 1);
    }

    @Override
    public void add(TDigest other) {
        for (Centroid centroid : other.centroids()) {
            add(centroid.mean(), centroid.count(), centroid);
        }
    }

    protected Centroid createCentroid(double mean, int id) {
        return new Centroid(mean, id, recordAllData);
    }
}
