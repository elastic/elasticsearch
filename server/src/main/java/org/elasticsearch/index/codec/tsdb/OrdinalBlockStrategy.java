/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * One strategy for encoding a block of sorted-field ordinals. Each strategy reports the exact size it
 * would occupy for a block, so {@link RunLengthOrdinalEncoder} can pick the smallest without
 * trial-encoding any candidate. The winner writes a header whose number of trailing one bits identifies
 * it (single-run 0, run-length 1, cyclic 2, two-run 3, bit-packing 4), and {@link RunLengthOrdinalDecoder}
 * dispatches back to it by that tag. Shorter tags go to the strategies whose header payload is small and
 * common, so they stay one byte; bit-packing carries none, so it takes the longest tag.
 * <p>
 * Implementations are stateless singletons; the per-encoder {@link DocValuesForUtil} is passed in rather
 * than held so a single instance can be reused across encoders and block sizes.
 */
sealed interface OrdinalBlockStrategy {

    /** Returned by {@link #encodedSize} when a strategy cannot encode the block. */
    long NOT_APPLICABLE = Long.MAX_VALUE;

    /**
     * Computes the exact number of bytes this strategy would write for the block, without encoding it.
     *
     * @param stats      the run structure of the block
     * @param bitsPerOrd bits required to represent the largest ordinal in the block
     * @param blockSize  number of ordinals in the block
     * @return the byte size, or {@link #NOT_APPLICABLE} if this strategy cannot encode the block
     */
    long encodedSize(Stats stats, int bitsPerOrd, int blockSize);

    /**
     * Writes the block, including its strategy header, to {@code out}.
     *
     * @param values     the ordinals to encode
     * @param stats      the run structure of the block
     * @param bitsPerOrd bits required to represent the largest ordinal in the block
     * @param forUtil    bit-packing helper, used only by {@link BitPacked}
     * @param out        the output to write to
     * @throws IOException if writing to {@code out} fails
     */
    void encode(long[] values, Stats stats, int bitsPerOrd, DocValuesForUtil forUtil, DataOutput out) throws IOException;

    /**
     * Reads a block previously written by {@link #encode} into {@code out}.
     *
     * @param headerPayload the block header with its trailing strategy bits already removed
     * @param bitsPerOrd    bits required to represent the largest ordinal in the block
     * @param in            the input positioned immediately after the header
     * @param out           the array to fill with decoded ordinals
     * @param forUtil       bit-packing helper, used only by {@link BitPacked}
     * @throws IOException if reading from {@code in} fails
     */
    void decode(long headerPayload, int bitsPerOrd, DataInput in, long[] out, DocValuesForUtil forUtil) throws IOException;

    /**
     * Run and cycle structure of a block, computed once and shared across strategies to size them.
     *
     * @param runCount        number of runs of equal consecutive ordinals
     * @param firstRunLength  length of the first run
     * @param firstValue      ordinal of the first run
     * @param lastValue       ordinal of the last run
     * @param runBodyBytes    bytes the run-length body (every run's value and length) would occupy
     * @param cyclicPeriod    smallest period that tiles the block, or {@code 0} if it is not cyclic
     * @param cyclicBodyBytes bytes the {@code cyclicPeriod} cycle values would occupy
     */
    record Stats(
        int runCount,
        int firstRunLength,
        long firstValue,
        long lastValue,
        long runBodyBytes,
        int cyclicPeriod,
        long cyclicBodyBytes
    ) {

        /**
         * Scans a block once to compute its {@link Stats}, including a candidate cycle period that is
         * then verified to tile the block. A repeated multi-valued set produces a cyclic block, so this
         * is the multi-valued analog of a run.
         *
         * @param values the ordinals to analyze
         * @return the run and cycle structure of {@code values}
         */
        static Stats of(final long[] values) {
            final int length = values.length;
            final long firstValue = values[0];

            long constantCheck = 0;
            for (int i = 1; i < length; i++) {
                constantCheck |= values[i] ^ firstValue;
            }
            if (constantCheck == 0) {
                return new Stats(1, length, firstValue, firstValue, zLongBytes(firstValue) + vIntBytes(length), 0, 0);
            }

            int runCount = 1;
            int firstRunLength = length;
            long runBodyBytes = 0;
            long runValue = firstValue;
            int runStart = 0;
            long previousRunValue = 0;
            int cycleCandidate = 0;
            for (int i = 1; i <= length; i++) {
                if (i == length || values[i] != runValue) {
                    if (runStart == 0) {
                        firstRunLength = i - runStart;
                    }
                    runBodyBytes += zLongBytes(runValue - previousRunValue) + vIntBytes(i - runStart);
                    previousRunValue = runValue;
                    if (i < length) {
                        runCount++;
                        runValue = values[i];
                        runStart = i;
                    }
                }
                if (cycleCandidate != -1 && i < length && values[i] == firstValue) {
                    if (cycleCandidate == 0) {
                        cycleCandidate = i;
                    } else if (cycleCandidate == 1 || i % cycleCandidate != 0) {
                        cycleCandidate = -1;
                    }
                }
            }

            int cyclicPeriod = 0;
            long cyclicBodyBytes = 0;
            if (cycleCandidate > 1 && tiles(values, cycleCandidate)) {
                cyclicPeriod = cycleCandidate;
                for (int i = 0; i < cycleCandidate; i++) {
                    cyclicBodyBytes += vLongBytes(values[i]);
                }
            }
            return new Stats(runCount, firstRunLength, firstValue, values[length - 1], runBodyBytes, cyclicPeriod, cyclicBodyBytes);
        }

        private static boolean tiles(final long[] values, int period) {
            for (int i = period; i < values.length; i++) {
                if (values[i] != values[i - period]) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Single run: the whole block is one value. Header is {@code value << 1} (zero trailing ones). */
    final class SingleRun implements OrdinalBlockStrategy {

        static final SingleRun INSTANCE = new SingleRun();

        private SingleRun() {}

        @Override
        public long encodedSize(final Stats stats, int bitsPerOrd, int blockSize) {
            if (stats.runCount() != 1 || bitsPerOrd >= 63) {
                return NOT_APPLICABLE;
            }
            return vLongBytes(stats.firstValue() << 1);
        }

        @Override
        public void encode(final long[] values, final Stats stats, int bitsPerOrd, final DocValuesForUtil forUtil, final DataOutput out)
            throws IOException {
            out.writeVLong(stats.firstValue() << 1);
        }

        @Override
        public void decode(long headerPayload, int bitsPerOrd, final DataInput in, final long[] out, final DocValuesForUtil forUtil) {
            Arrays.fill(out, headerPayload);
        }
    }

    /** Two runs: a first run of one value then a second run of another. Header sets three trailing ones. */
    final class TwoRun implements OrdinalBlockStrategy {

        static final TwoRun INSTANCE = new TwoRun();

        private TwoRun() {}

        @Override
        public long encodedSize(final Stats stats, int bitsPerOrd, int blockSize) {
            if (stats.runCount() != 2 || bitsPerOrd >= 60) {
                return NOT_APPLICABLE;
            }
            return vLongBytes((stats.firstValue() << 4) | 0b0111) + vIntBytes(stats.firstRunLength()) + zLongBytes(
                stats.lastValue() - stats.firstValue()
            );
        }

        @Override
        public void encode(final long[] values, final Stats stats, int bitsPerOrd, final DocValuesForUtil forUtil, final DataOutput out)
            throws IOException {
            out.writeVLong((stats.firstValue() << 4) | 0b0111);
            out.writeVInt(stats.firstRunLength());
            out.writeZLong(stats.lastValue() - stats.firstValue());
        }

        @Override
        public void decode(long headerPayload, int bitsPerOrd, final DataInput in, final long[] out, final DocValuesForUtil forUtil)
            throws IOException {
            final int firstRunLength = in.readVInt();
            final long secondValue = headerPayload + in.readZLong();
            Arrays.fill(out, 0, firstRunLength, headerPayload);
            Arrays.fill(out, firstRunLength, out.length, secondValue);
        }
    }

    /** Bit-packing: the always-applicable fallback for high-entropy blocks. Sets four trailing ones. */
    final class BitPacked implements OrdinalBlockStrategy {

        static final BitPacked INSTANCE = new BitPacked();

        private BitPacked() {}

        @Override
        public long encodedSize(final Stats stats, int bitsPerOrd, int blockSize) {
            return 1 + (long) blockSize * bitsPerOrd / 8;
        }

        @Override
        public void encode(final long[] values, final Stats stats, int bitsPerOrd, final DocValuesForUtil forUtil, final DataOutput out)
            throws IOException {
            out.writeVLong(0b01111);
            forUtil.encode(values, bitsPerOrd, out);
        }

        @Override
        public void decode(long headerPayload, int bitsPerOrd, final DataInput in, final long[] out, final DocValuesForUtil forUtil)
            throws IOException {
            forUtil.decode(bitsPerOrd, in, out);
        }
    }

    /** Run-length: header carries the run count, then each run is a zigzag delta value and a length. */
    final class RunLength implements OrdinalBlockStrategy {

        static final RunLength INSTANCE = new RunLength();

        private RunLength() {}

        @Override
        public long encodedSize(final Stats stats, int bitsPerOrd, int blockSize) {
            return vLongBytes(((long) stats.runCount() << 2) | 0b01) + stats.runBodyBytes();
        }

        @Override
        public void encode(final long[] values, final Stats stats, int bitsPerOrd, final DocValuesForUtil forUtil, final DataOutput out)
            throws IOException {
            out.writeVLong(((long) stats.runCount() << 2) | 0b01);
            long previousValue = 0;
            int i = 0;
            while (i < values.length) {
                final long value = values[i];
                int next = i + 1;
                while (next < values.length && values[next] == value) {
                    next++;
                }
                out.writeZLong(value - previousValue);
                out.writeVInt(next - i);
                previousValue = value;
                i = next;
            }
        }

        @Override
        public void decode(long headerPayload, int bitsPerOrd, final DataInput in, final long[] out, final DocValuesForUtil forUtil)
            throws IOException {
            final int runCount = (int) headerPayload;
            long value = 0;
            int position = 0;
            for (int run = 0; run < runCount; run++) {
                value += in.readZLong();
                final int runLength = in.readVInt();
                Arrays.fill(out, position, position + runLength, value);
                position += runLength;
            }
            assert position == out.length : position;
        }
    }

    /** Cyclic: the block tiles a short repeating set of values, as a repeated multi-valued set does. */
    final class Cyclic implements OrdinalBlockStrategy {

        static final Cyclic INSTANCE = new Cyclic();

        private Cyclic() {}

        @Override
        public long encodedSize(final Stats stats, int bitsPerOrd, int blockSize) {
            if (stats.cyclicPeriod() == 0) {
                return NOT_APPLICABLE;
            }
            return vLongBytes(((long) stats.cyclicPeriod() << 3) | 0b011) + stats.cyclicBodyBytes();
        }

        @Override
        public void encode(final long[] values, final Stats stats, int bitsPerOrd, final DocValuesForUtil forUtil, final DataOutput out)
            throws IOException {
            final int period = stats.cyclicPeriod();
            out.writeVLong(((long) period << 3) | 0b011);
            for (int i = 0; i < period; i++) {
                out.writeVLong(values[i]);
            }
        }

        @Override
        public void decode(long headerPayload, int bitsPerOrd, final DataInput in, final long[] out, final DocValuesForUtil forUtil)
            throws IOException {
            final int period = (int) headerPayload;
            for (int i = 0; i < period; i++) {
                out[i] = in.readVLong();
            }
            int length = period;
            while (length < out.length) {
                final int copyLength = Math.min(length, out.length - length);
                System.arraycopy(out, 0, out, length, copyLength);
                length += copyLength;
            }
        }
    }

    static int vLongBytes(long value) {
        int bytes = 1;
        while ((value & ~0x7FL) != 0L) {
            value >>>= 7;
            bytes++;
        }
        return bytes;
    }

    static int zLongBytes(long value) {
        return vLongBytes((value << 1) ^ (value >> 63));
    }

    static int vIntBytes(int value) {
        int bytes = 1;
        while ((value & ~0x7F) != 0) {
            value >>>= 7;
            bytes++;
        }
        return bytes;
    }
}
