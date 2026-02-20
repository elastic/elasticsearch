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

import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Read-only digest backed by encoded centroid bytes.
 * <p>
 * The encoded format is a sequence of pairs:
 * <ul>
 *     <li>centroid count as VLong</li>
 *     <li>centroid mean as IEEE754 double (8 bytes, big-endian)</li>
 * </ul>
 */
public final class EncodedTDigest implements ReadableTDigest {

    private final BytesRef encodedDigest = new BytesRef();
    private long cachedSize = -1L;
    private double cachedMax = Double.NaN;
    private int cachedCentroidCount = -1;

    public EncodedTDigest() {}

    public EncodedTDigest(BytesRef encodedDigest) {
        reset(encodedDigest);
    }

    /**
     * Replaces the underlying encoded digest bytes.
     * All decoding happens lazily when methods like {@link #size()} or {@link #cdf(double)} are called.
     * The provided {@code encodedDigest} is copied shallowly, so the caller is responsible for ensuring
     * that the bytes remain unchanged for the lifetime of this instance.
     *
     * @param encodedDigest The new encoded digest bytes. Must not be null, but may be empty.
     */
    public void reset(BytesRef encodedDigest) {
        this.encodedDigest.bytes = encodedDigest.bytes;
        this.encodedDigest.offset = encodedDigest.offset;
        this.encodedDigest.length = encodedDigest.length;
        this.cachedSize = -1L;
        this.cachedMax = Double.NaN;
        this.cachedCentroidCount = -1;
    }

    /**
     * Returns the currently configured encoded digest bytes.
     */
    public BytesRef encodedDigest() {
        return encodedDigest;
    }

    /**
     * Returns an allocation-free iterator over encoded centroids.
     */
    public CentroidIterator centroidIterator() {
        return new EncodedCentroidIterator(encodedDigest);
    }

    /**
     * Encodes the provided centroids into a {@link BytesRef}.
     */
    public static BytesRef encodeCentroids(List<? extends Centroid> centroids) {
        return encodeCentroidsFromIterator(new CentroidIterator() {
            private int index = -1;

            @Override
            public boolean next() {
                index++;
                return index < centroids.size();
            }

            @Override
            public long currentCount() {
                return centroids.get(index).count();
            }

            @Override
            public double currentMean() {
                return centroids.get(index).mean();
            }

            @Override
            public boolean endReached() {
                return index + 1 >= centroids.size();
            }

        });
    }

    /**
     * Encodes centroids represented by independent means and counts lists.
     */
    public static BytesRef encodeCentroids(List<Double> means, List<Long> counts) {
        assert means.size() == counts.size() : "centroids and counts must have equal size";
        return encodeCentroidsFromIterator(new CentroidIterator() {
            private int index = -1;

            @Override
            public boolean next() {
                index++;
                return index < means.size();
            }

            @Override
            public long currentCount() {
                return counts.get(index);
            }

            @Override
            public double currentMean() {
                return means.get(index);
            }

            @Override
            public boolean endReached() {
                return index + 1 >= means.size();
            }
        });
    }

    @Override
    public long size() {
        ensureCachedStatsPopulated();
        return cachedSize;
    }

    @Override
    public double cdf(double x) {
        TDigest.checkValue(x);
        long digestSize = size();
        if (digestSize == 0) {
            return Double.NaN;
        }
        CentroidIterator centroids = centroidIterator();
        if (centroids.next() == false) {
            return Double.NaN;
        }
        double firstMean = centroids.currentMean();
        long firstCount = centroids.currentCount();
        if (centroids.next() == false) {
            if (x < firstMean) {
                return 0;
            }
            if (x > firstMean) {
                return 1;
            }
            return 0.5;
        }

        double min = firstMean;
        double max = getMax();
        if (x < min) {
            return 0;
        }
        if (Double.compare(x, min) == 0) {
            double dw = firstCount;
            while (Double.compare(centroids.currentMean(), x) == 0) {
                dw += centroids.currentCount();
                if (centroids.next() == false) {
                    break;
                }
            }
            return dw / 2.0 / digestSize;
        }

        if (x > max) {
            return 1;
        }
        if (Double.compare(x, max) == 0) {
            double dw = 0;
            CentroidIterator it = centroidIterator();
            while (it.next()) {
                if (Double.compare(it.currentMean(), x) == 0) {
                    dw += it.currentCount();
                }
            }
            return (digestSize - dw / 2.0) / digestSize;
        }

        double aMean = firstMean;
        long aCount = firstCount;
        double bMean = centroids.currentMean();
        long bCount = centroids.currentCount();
        boolean hasMore = centroids.next();

        double left = (bMean - aMean) / 2;
        double right = left;
        double weightSoFar = 0;
        while (hasMore) {
            if (x < aMean + right) {
                double value = (weightSoFar + aCount * AbstractTDigest.interpolate(x, aMean - left, aMean + right)) / digestSize;
                return Math.max(value, 0.0);
            }
            weightSoFar += aCount;
            aMean = bMean;
            aCount = bCount;
            left = right;
            bMean = centroids.currentMean();
            bCount = centroids.currentCount();
            hasMore = centroids.next();
            right = (bMean - aMean) / 2;
        }
        if (x < aMean + right) {
            return (weightSoFar + aCount * AbstractTDigest.interpolate(x, aMean - right, aMean + right)) / digestSize;
        }
        return 1;
    }

    @Override
    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }
        long digestSize = size();
        CentroidIterator centroids = centroidIterator();
        if (digestSize == 0) {
            return Double.NaN;
        }
        if (centroids.next() == false) {
            return Double.NaN;
        }
        double currentMean = centroids.currentMean();
        long currentWeight = centroids.currentCount();
        if (centroids.next() == false) {
            return currentMean;
        }

        double target = q * digestSize;
        double min = getMin();
        if (target <= 0) {
            return min;
        }
        double max = getMax();
        if (target >= digestSize) {
            return max;
        }

        double weightSoFar = currentWeight / 2.0;
        if (target <= weightSoFar && weightSoFar > 1) {
            return AbstractTDigest.weightedAverage(min, weightSoFar - target, currentMean, target);
        }
        do {
            double nextMean = centroids.currentMean();
            long nextWeight = centroids.currentCount();
            double dw = (currentWeight + nextWeight) / 2.0;
            if (target < weightSoFar + dw) {
                double w1 = target - weightSoFar;
                double w2 = weightSoFar + dw - target;
                return AbstractTDigest.weightedAverage(currentMean, w2, nextMean, w1);
            }
            weightSoFar += dw;
            currentMean = nextMean;
            currentWeight = nextWeight;
        } while (centroids.next());
        double w1 = target - weightSoFar;
        double w2 = currentWeight / 2.0 - w1;
        return AbstractTDigest.weightedAverage(currentMean, w2, max, w1);
    }

    @Override
    public Collection<Centroid> centroids() {
        if (encodedDigest.length == 0) {
            return List.of();
        }
        List<Centroid> decoded = new ArrayList<>();
        CentroidIterator it = centroidIterator();
        while (it.next()) {
            decoded.add(new Centroid(it.currentMean(), it.currentCount()));
        }
        return Collections.unmodifiableList(decoded);
    }

    @Override
    public int centroidCount() {
        ensureCachedStatsPopulated();
        return cachedCentroidCount;
    }

    @Override
    public double getMin() {
        CentroidIterator iterator = centroidIterator();
        if (iterator.next() == false) {
            return Double.NaN;
        }
        return iterator.currentMean();
    }

    @Override
    public double getMax() {
        ensureCachedStatsPopulated();
        return cachedMax;
    }

    private void ensureCachedStatsPopulated() {
        if (cachedSize >= 0) {
            return;
        }
        long size = 0L;
        double max = Double.NaN;
        int centroidCount = 0;
        CentroidIterator it = centroidIterator();
        while (it.next()) {
            size += it.currentCount();
            max = it.currentMean();
            centroidCount++;
        }
        cachedSize = size;
        cachedMax = max;
        cachedCentroidCount = centroidCount;
    }

    /**
     * An iterator over encoded t-digest centroids.
     */
    public interface CentroidIterator {
        boolean next();

        long currentCount();

        double currentMean();

        /**
         * Returns {@code true} iff a call to {@link #next()} would return {@code false}.
         */
        boolean endReached();
    }

    private static BytesRef encodeCentroidsFromIterator(CentroidIterator centroids) {
        AccessibleByteArrayOutputStream out = new AccessibleByteArrayOutputStream(32);
        while (centroids.next()) {
            long count = centroids.currentCount();
            if (count < 0) {
                throw new IllegalArgumentException("Centroid count cannot be negative: " + count);
            }
            if (count > 0) {
                try {
                    writeVLong(count, out);
                    writeDouble(centroids.currentMean(), out);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to encode centroid", e);
                }
            }
        }
        return new BytesRef(out.getBufferDirect(), 0, out.size());
    }

    private static final class EncodedCentroidIterator implements CentroidIterator {
        private final AccessibleByteArrayStreamInput input;

        private long count;
        private double mean;

        private EncodedCentroidIterator(BytesRef encodedDigest) {
            this.input = new AccessibleByteArrayStreamInput(encodedDigest.bytes, encodedDigest.offset, encodedDigest.length);
            this.count = -1;
        }

        @Override
        public boolean next() {
            if (endReached()) {
                return false;
            }
            count = input.readVLong();
            mean = input.readDouble();
            return true;
        }

        @Override
        public long currentCount() {
            assert count != -1 : "next() must be called and return true before accessing current centroid";
            return count;
        }

        @Override
        public double currentMean() {
            assert count != -1 : "next() must be called and return true before accessing current centroid";
            return mean;
        }

        @Override
        public boolean endReached() {
            return input.available() == 0;
        }
    }

    private static class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {
        AccessibleByteArrayOutputStream(int size) {
            super(size);
        }

        byte[] getBufferDirect() {
            return buf;
        }
    }

    private static class AccessibleByteArrayStreamInput extends ByteArrayInputStream {
        AccessibleByteArrayStreamInput(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        long readVLong() {
            byte b = (byte) read();
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            if (b != 0 && b != 1) {
                throw new IllegalStateException("Broken VLong detected");
            }
            i |= ((long) b) << 63;
            return i;
        }

        double readDouble() {
            if (available() < Double.BYTES) {
                throw new IllegalStateException("Malformed TDigest bytes");
            }
            long raw = ((long) read() & 0xFF) << 56;
            raw |= ((long) read() & 0xFF) << 48;
            raw |= ((long) read() & 0xFF) << 40;
            raw |= ((long) read() & 0xFF) << 32;
            raw |= ((long) read() & 0xFF) << 24;
            raw |= ((long) read() & 0xFF) << 16;
            raw |= ((long) read() & 0xFF) << 8;
            raw |= ((long) read() & 0xFF);
            return Double.longBitsToDouble(raw);
        }
    }

    private static void writeVLong(long value, OutputStream out) throws IOException {
        while ((value & ~0x7FL) != 0L) {
            out.write((byte) ((value & 0x7FL) | 0x80L));
            value >>>= 7;
        }
        out.write((byte) value);
    }

    private static void writeDouble(double value, OutputStream out) throws IOException {
        long bits = Double.doubleToRawLongBits(value);
        out.write((int) ((bits >>> 56) & 0xFF));
        out.write((int) ((bits >>> 48) & 0xFF));
        out.write((int) ((bits >>> 40) & 0xFF));
        out.write((int) ((bits >>> 32) & 0xFF));
        out.write((int) ((bits >>> 24) & 0xFF));
        out.write((int) ((bits >>> 16) & 0xFF));
        out.write((int) ((bits >>> 8) & 0xFF));
        out.write((int) (bits & 0xFF));
    }
}
