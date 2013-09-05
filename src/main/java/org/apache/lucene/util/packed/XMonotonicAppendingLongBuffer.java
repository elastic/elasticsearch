package org.apache.lucene.util.packed;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import java.util.Arrays;

/**
 * Utility class to buffer signed longs in memory, which is optimized for the
 * case where the sequence is monotonic, although it can encode any sequence of
 * arbitrary longs. It only supports appending.
 *
 * @lucene.internal
 */
public final class XMonotonicAppendingLongBuffer extends XAbstractAppendingLongBuffer {
    static {
        // LUCENE MONITOR: this should be in Lucene 4.5.
        assert Lucene.VERSION == Version.LUCENE_44 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

    static long zigZagDecode(long n) {
        return ((n >>> 1) ^ -(n & 1));
    }

    static long zigZagEncode(long n) {
        return (n >> 63) ^ (n << 1);
    }

    float[] averages;
    long[] minValues;

    /**
     * @param initialPageCount        the initial number of pages
     * @param pageSize                the size of a single page
     * @param acceptableOverheadRatio an acceptable overhead ratio per value
     */
    public XMonotonicAppendingLongBuffer(int initialPageCount, int pageSize, float acceptableOverheadRatio) {
        super(initialPageCount, pageSize, acceptableOverheadRatio);
        averages = new float[values.length];
        minValues = new long[values.length];
    }

    /**
     * Create an {@link MonotonicAppendingLongBuffer} with initialPageCount=16,
     * pageSize=1024 and acceptableOverheadRatio={@link PackedInts#DEFAULT}
     */
    public XMonotonicAppendingLongBuffer() {
        this(16, 1024, PackedInts.DEFAULT);
    }

    /**
     * Create an {@link MonotonicAppendingLongBuffer} with initialPageCount=16,
     * pageSize=1024
     */
    public XMonotonicAppendingLongBuffer(float acceptableOverheadRatio) {
        this(16, 1024, acceptableOverheadRatio);
    }


    @Override
    long get(int block, int element) {
        if (block == valuesOff) {
            return pending[element];
        } else {
            final long base = minValues[block] + (long) (averages[block] * (long) element);
            if (values[block] == null) {
                return base;
            } else {
                return base + zigZagDecode(values[block].get(element));
            }
        }
    }

    @Override
    int get(int block, int element, long[] arr, int off, int len) {
        if (block == valuesOff) {
            int sysCopyToRead = Math.min(len, pendingOff - element);
            System.arraycopy(pending, element, arr, off, sysCopyToRead);
            return sysCopyToRead;
        } else {
            if (values[block] == null) {
                int toFill = Math.min(len, pending.length - element);
                for (int r = 0; r < toFill; r++, off++, element++) {
                    arr[off] = minValues[block] + (long) (averages[block] * (long) element);
                }
                return toFill;
            } else {

    /* packed block */
                int read = values[block].get(element, arr, off, len);
                for (int r = 0; r < read; r++, off++, element++) {
                    arr[off] = minValues[block] + (long) (averages[block] * (long) element) + zigZagDecode(arr[off]);
                }
                return read;
            }
        }
    }

    @Override
    void grow(int newBlockCount) {
        super.grow(newBlockCount);
        this.averages = Arrays.copyOf(averages, newBlockCount);
        this.minValues = Arrays.copyOf(minValues, newBlockCount);
    }

    @Override
    void packPendingValues() {
        assert pendingOff > 0;
        minValues[valuesOff] = pending[0];
        averages[valuesOff] = pendingOff == 1 ? 0 : (float) (pending[pendingOff - 1] - pending[0]) / (pendingOff - 1);

        for (int i = 0; i < pendingOff; ++i) {
            pending[i] = zigZagEncode(pending[i] - minValues[valuesOff] - (long) (averages[valuesOff] * (long) i));
        }
        long maxDelta = 0;
        for (int i = 0; i < pendingOff; ++i) {
            if (pending[i] < 0) {
                maxDelta = -1;
                break;
            } else {
                maxDelta = Math.max(maxDelta, pending[i]);
            }
        }
        if (maxDelta == 0) {
            values[valuesOff] = new PackedInts.NullReader(pendingOff);
        } else {
            final int bitsRequired = maxDelta < 0 ? 64 : PackedInts.bitsRequired(maxDelta);
            final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired, acceptableOverheadRatio);
            for (int i = 0; i < pendingOff; ) {
                i += mutable.set(i, pending, i, pendingOff - i);
            }
            values[valuesOff] = mutable;
        }
    }

    @Override
    long baseRamBytesUsed() {
        return super.baseRamBytesUsed()
                + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // 2 additional arrays
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed()
                + RamUsageEstimator.sizeOf(averages) + RamUsageEstimator.sizeOf(minValues);
    }

}
