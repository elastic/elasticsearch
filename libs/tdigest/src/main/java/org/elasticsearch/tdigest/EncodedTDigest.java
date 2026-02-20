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
import java.util.Objects;

/**
 * Read-only digest backed by encoded centroid bytes.
 * <p>
 * The encoded format is a sequence of pairs:
 * <ul>
 *     <li>centroid count as VLong</li>
 *     <li>centroid mean as IEEE754 double (8 bytes, big-endian)</li>
 * </ul>
 */
public class EncodedTDigest implements ReadableTDigest {

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
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < centroids.size();
            }

            @Override
            public long peekCount() {
                return centroids.get(index).count();
            }

            @Override
            public double peekMean() {
                return centroids.get(index).mean();
            }

            @Override
            public void advance() {
                index++;
            }

        });
    }

    /**
     * Encodes centroids represented by independent means and counts lists.
     */
    public static BytesRef encodeCentroids(List<Double> means, List<Long> counts) {
        assert  means.size() == counts.size() : "centroids and counts must have equal size";
        return encodeCentroidsFromIterator(new CentroidIterator() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < means.size();
            }

            @Override
            public long peekCount() {
                return counts.get(index);
            }

            @Override
            public double peekMean() {
                return means.get(index);
            }

            @Override
            public void advance() {
                index++;
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
        double firstMean = centroids.peekMean();
        long firstCount = centroids.peekCount();
        centroids.advance();
        if (centroids.hasNext() == false) {
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
            while (centroids.hasNext() && Double.compare(centroids.peekMean(), x) == 0) {
                dw += centroids.peekCount();
                centroids.advance();
            }
            return dw / 2.0 / digestSize;
        }

        if (x > max) {
            return 1;
        }
        if (Double.compare(x, max) == 0) {
            double dw = 0;
            for (CentroidIterator it = centroidIterator(); it.hasNext(); it.advance()) {
                if (Double.compare(it.peekMean(), x) == 0) {
                    dw += it.peekCount();
                }
            }
            return (digestSize - dw / 2.0) / digestSize;
        }

        double aMean = firstMean;
        long aCount = firstCount;
        double bMean = centroids.peekMean();
        long bCount = centroids.peekCount();
        centroids.advance();

        double left = (bMean - aMean) / 2;
        double right = left;
        double weightSoFar = 0;
        while (centroids.hasNext()) {
            if (x < aMean + right) {
                double value = (weightSoFar + aCount * AbstractTDigest.interpolate(x, aMean - left, aMean + right)) / digestSize;
                return Math.max(value, 0.0);
            }
            weightSoFar += aCount;
            aMean = bMean;
            aCount = bCount;
            left = right;
            bMean = centroids.peekMean();
            bCount = centroids.peekCount();
            centroids.advance();
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
        double currentMean = centroids.peekMean();
        long currentWeight = centroids.peekCount();
        centroids.advance();
        if (centroids.hasNext() == false) {
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
        while (centroids.hasNext()) {
            double nextMean = centroids.peekMean();
            long nextWeight = centroids.peekCount();
            centroids.advance();
            double dw = (currentWeight + nextWeight) / 2.0;
            if (target < weightSoFar + dw) {
                double w1 = target - weightSoFar;
                double w2 = weightSoFar + dw - target;
                return AbstractTDigest.weightedAverage(currentMean, w2, nextMean, w1);
            }
            weightSoFar += dw;
            currentMean = nextMean;
            currentWeight = nextWeight;
        }
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
        for (CentroidIterator it = centroidIterator(); it.hasNext(); it.advance()) {
            decoded.add(new Centroid(it.peekMean(), it.peekCount()));
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
        if (iterator.hasNext() == false) {
            return Double.NaN;
        }
        return iterator.peekMean();
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
        for (CentroidIterator it = centroidIterator(); it.hasNext(); it.advance()) {
            size += it.peekCount();
            max = it.peekMean();
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
        boolean hasNext();

        long peekCount();

        double peekMean();

        void advance();
    }

    private static BytesRef encodeCentroidsFromIterator(CentroidIterator centroids) {
        AccessibleByteArrayOutputStream out = new AccessibleByteArrayOutputStream(32);
        while (centroids.hasNext()) {
            long count = centroids.peekCount();
            if (count < 0) {
                throw new IllegalArgumentException("Centroid count cannot be negative: " + count);
            }
            if (count > 0) {
                try {
                    writeVLong(count, out);
                    writeDouble(centroids.peekMean(), out);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to encode centroid", e);
                }
            }
            centroids.advance();
        }
        return new BytesRef(out.getBufferDirect(), 0, out.size());
    }

    private static final class EncodedCentroidIterator implements CentroidIterator {
        private final AccessibleByteArrayStreamInput input;

        private boolean hasNext;
        private long count;
        private double mean;

        private EncodedCentroidIterator(BytesRef encodedDigest) {
            this.input = new AccessibleByteArrayStreamInput(encodedDigest.bytes, encodedDigest.offset, encodedDigest.length);
            decodeNext();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public long peekCount() {
            ensureHasNext();
            return count;
        }

        @Override
        public double peekMean() {
            ensureHasNext();
            return mean;
        }

        @Override
        public void advance() {
            ensureHasNext();
            decodeNext();
        }

        private void decodeNext() {
            while (input.available() > 0) {
                long decodedCount = input.readVLong();
                double decodedMean = input.readDouble();
                if (decodedCount == 0) {
                    continue;
                }
                count = decodedCount;
                mean = decodedMean;
                hasNext = true;
                return;
            }
            hasNext = false;
            count = 0L;
            mean = Double.NaN;
        }

        private void ensureHasNext() {
            if (hasNext == false) {
                throw new IllegalStateException("Iterator exhausted");
            }
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
