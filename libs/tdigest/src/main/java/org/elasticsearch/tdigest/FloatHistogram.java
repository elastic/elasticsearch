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

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Maintains histogram buckets that are constant width
 * in base-2 floating point representation space. This is close
 * to exponential binning, but should be much faster.
 */
public class FloatHistogram extends Histogram {
    private int bitsOfPrecision;
    private int shift;
    private int offset;


    @SuppressWarnings("WeakerAccess")
    public FloatHistogram(double min, double max) {
        this(min, max, 50);
    }

    @SuppressWarnings("WeakerAccess")
    public FloatHistogram(double min, double max, double binsPerDecade) {
        super(min, max);
        if (max <= 2 * min) {
            throw new IllegalArgumentException(String.format("Illegal/nonsensical min, max (%.2f, %.2g)", min, max));
        }
        if (min <= 0 || max <= 0) {
            throw new IllegalArgumentException("Min and max must be positive");
        }
        if (binsPerDecade < 5 || binsPerDecade > 10000) {
            throw new IllegalArgumentException(
                    String.format("Unreasonable number of bins per decade %.2g. Expected value in range [5,10000]",
                            binsPerDecade));
        }

        // convert binsPerDecade into bins per octave, then figure out how many bits that takes
        bitsOfPrecision = (int) Math.ceil(Math.log(binsPerDecade * Math.log10(2)) / Math.log(2));
        // we keep just the required amount of the mantissa
        shift = 52 - bitsOfPrecision;
        // The exponent in a floating point number is offset
        offset = 0x3ff << bitsOfPrecision;

        setupBins(min, max);
    }

    @Override
    protected int bucketIndex(double x) {
        x = x / min;
        long floatBits = Double.doubleToLongBits(x);
        return (int) (floatBits >>> shift) - offset;
    }

    // exposed for testing
    @Override
    double lowerBound(int k) {
        return min * Double.longBitsToDouble((k + (0x3ffL << bitsOfPrecision)) << (52 - bitsOfPrecision)) /* / fuzz */;
    }

    @Override
    @SuppressWarnings("WeakerAccess")
    public long[] getCompressedCounts() {
        LongBuffer buf = LongBuffer.allocate(counts.length);
        Simple64.compress(buf, counts, 0, counts.length);
        long[] r = new long[buf.position()];
        buf.flip();
        buf.get(r);
        return r;
    }

    @Override
    @SuppressWarnings("WeakerAccess")
    public void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeByte(bitsOfPrecision);
        out.writeByte(shift);

        ByteBuffer buf = ByteBuffer.allocate(8 * counts.length);
        LongBuffer longBuffer = buf.asLongBuffer();
        Simple64.compress(longBuffer, counts, 0, counts.length);
        buf.position(8 * longBuffer.position());
        byte[] r = new byte[buf.position()];
        out.writeShort(buf.position());
        buf.flip();
        buf.get(r);
        out.write(r);
    }

    @Override
    @SuppressWarnings("WeakerAccess")
    public void readObject(java.io.ObjectInputStream in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
        bitsOfPrecision = in.readByte();
        shift = in.readByte();
        offset = 0x3ff << bitsOfPrecision;

        int n = in.readShort();
        ByteBuffer buf = ByteBuffer.allocate(n);
        in.readFully(buf.array(), 0, n);
        int binCount = bucketIndex(max) + 1;
        if (binCount > 10000) {
            throw new IllegalArgumentException(
                    String.format("Excessive number of bins %d during deserialization = %.2g, %.2g",
                            binCount, min, max));

        }
        counts = new long[binCount];
        Simple64.decompress(buf.asLongBuffer(), counts);
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("Stream data required");
    }

    @Override
    void add(Iterable<Histogram> others) {
        for (Histogram other : others) {
            if (!this.getClass().equals(other.getClass())) {
                throw new IllegalArgumentException(String.format("Cannot add %s to FloatHistogram", others.getClass()));
            }
            FloatHistogram actual = (FloatHistogram) other;
            if (actual.min != min || actual.max != max || actual.counts.length != counts.length) {
                throw new IllegalArgumentException("Can only merge histograms with identical bounds and precision");
            }
            for (int i = 0; i < counts.length; i++) {
                counts[i] += other.counts[i];
            }
        }
    }
}
