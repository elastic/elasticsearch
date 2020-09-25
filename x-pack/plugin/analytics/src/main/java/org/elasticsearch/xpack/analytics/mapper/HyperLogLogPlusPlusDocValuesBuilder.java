/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.search.aggregations.metrics.AbstractLinearCounting;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValue;

import java.io.IOException;

/** read/write HyperLogLogPlusPlus doc values */
public class HyperLogLogPlusPlusDocValuesBuilder {

    // encoding modes for doc values
    private static final byte FIXED_LENGTH = 0;
    private static final byte RUN_LENGTH = 1;
    private static final byte CUSTOM = 2;
    private static final byte LC = 3;

    final InternalHyperLogLogPlusPlusValue counts;

    HyperLogLogPlusPlusDocValuesBuilder(int precision) {
        counts = new InternalHyperLogLogPlusPlusValue(precision);
    }

    public static void writeLC(IntArrayList hashes, ByteBuffersDataOutput dataOutput) throws IOException {
        dataOutput.writeByte(LC);
        final int size = hashes.size();
        dataOutput.writeVInt(size);
        // sort the values for delta encoding
        new IntroSorter() {
            int pivot;

            @Override
            protected void swap(int i, int j) {
               final int tmp = hashes.get(i);
               hashes.set(i, hashes.get(j));
               hashes.set(j, tmp);
            }

            @Override
            protected void setPivot(int i) {
                pivot = hashes.get(i);
            }

            @Override
            protected int comparePivot(int j) {
                return Integer.compare(pivot, hashes.get(j));
            }
        }.sort(0, size);
        if (size > 0) {
            dataOutput.writeInt(hashes.get(0));
            for (int i = 1; i < size; i++) {
                dataOutput.writeVLong((long) hashes.get(i) - (long) hashes.get(i - 1));
            }
        }
    }

    public static void writeHLL(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        // chose the encoding to use
        final int fixedLength = runLens.size();
        final int runLenLength = runLenLength(runLens);
        final int customLength = customLength(runLens);
        if (fixedLength <= runLenLength) {
            if (fixedLength <= customLength) {
                writeFixedLen(runLens, dataOutput);
                assert dataOutput.size() == fixedLength + 1;
            } else {
                writeCustom(runLens, dataOutput);
                assert dataOutput.size() == customLength + 1;
            }
        } else if (runLenLength < customLength) {
            writeRunLen(runLens, dataOutput);
            assert dataOutput.size() < fixedLength + 1;
        } else {
            writeCustom(runLens, dataOutput);
            assert dataOutput.size() == customLength + 1;
        }
    }

    private static void writeFixedLen(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) {
        dataOutput.writeByte(FIXED_LENGTH);
        for (int i = 0; i < runLens.size(); i++) {
            dataOutput.writeByte(runLens.get(i));
        }
    }

    private static int runLenLength(ByteArrayList runLens) {
        int compressionLength = 0;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value) {
                compressionLength += 2;
                value = nextValue;
            }
        }
        compressionLength += 2;
        return compressionLength;
    }

    private static void writeRunLen(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        dataOutput.writeByte(RUN_LENGTH);
        int length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value) {
                dataOutput.writeVInt(length);
                dataOutput.writeByte(value);
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        dataOutput.writeVInt(length);
        dataOutput.writeByte(value);
    }

    private static int customLength(ByteArrayList runLens) {
        int compressionLength = 0;
        byte length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value || length == Byte.MAX_VALUE) {
                if (length > 1) {
                    compressionLength++;
                }
                compressionLength++;
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        if (length > 1) {
            compressionLength++;
        }
        compressionLength++;
        return compressionLength;
    }

    private static void writeCustom(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) {
        dataOutput.writeByte(CUSTOM);
        byte length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value || length == Byte.MAX_VALUE) {
                if (length > 1) {
                    dataOutput.writeByte((byte) -length);
                }
                dataOutput.writeByte(value);
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        if (length > 1) {
            dataOutput.writeByte((byte) -length);
        }
        dataOutput.writeByte(value);
    }

    protected HyperLogLogPlusPlusValue decode(ByteArrayDataInput dataInput) throws IOException {
        counts.reset(dataInput);
        return counts;
    }

    /** re-usable {@link HyperLogLogPlusPlusValue} implementation */
    private static class InternalHyperLogLogPlusPlusValue extends HyperLogLogPlusPlusValue {

        private final FixedLengthHllValue fixedValue;
        private final RunLenHllValue runLenValue;
        private final CustomHllValue customValue;

        private final LcValue lc;
        private AbstractHyperLogLog.RunLenIterator hll;
        private Algorithm algorithm;

        InternalHyperLogLogPlusPlusValue(int precision) {
            fixedValue = new FixedLengthHllValue(precision);
            runLenValue = new RunLenHllValue(precision);
            customValue = new CustomHllValue(precision);
            lc = new LcValue(precision);
        }

        /** reset the value for the HyperLogLogPlusPlus sketch */
        void reset(ByteArrayDataInput dataInput) throws IOException {
            byte mode = dataInput.readByte();
            switch (mode) {
                case FIXED_LENGTH:
                    fixedValue.reset(dataInput);
                    hll = fixedValue;
                    algorithm = Algorithm.HYPERLOGLOG;
                    break;
                case RUN_LENGTH:
                    runLenValue.reset(dataInput);
                    hll = runLenValue;
                    algorithm = Algorithm.HYPERLOGLOG;
                    break;
                case CUSTOM:
                    customValue.reset(dataInput);
                    hll = customValue;
                    algorithm = Algorithm.HYPERLOGLOG;
                    break;
                case LC:
                    lc.reset(dataInput);
                    algorithm = Algorithm.LINEAR_COUNTING;
                    break;
                default:
                    throw new IOException("Unknown compression mode: " + mode);
            }
        }

        @Override
        public Algorithm getAlgorithm() {
            return algorithm;
        }

        @Override
        public AbstractLinearCounting.EncodedHashesIterator getLinearCounting() {
            return algorithm == Algorithm.LINEAR_COUNTING ? lc : null;
        }

        @Override
        public AbstractHyperLogLog.RunLenIterator getHyperLogLog() {
            return algorithm == Algorithm.HYPERLOGLOG ? hll : null;
        }
    }

    /** re-usable {@link AbstractHyperLogLog.RunLenIterator} implementation.
     * HLL sketch is compressed as fixed length array */
    private static class FixedLengthHllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private final int precision;

        FixedLengthHllValue(int precision) {
            this.precision = precision;
        }

        @Override
        public int precision() {
            return precision;
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
        }

        @Override
        public boolean next() {
            if (dataInput.eof() == false) {
                value = dataInput.readByte();
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            dataInput.skipBytes(bytes);
        }
    }

    /** re-usable {@link AbstractHyperLogLog.RunLenIterator} implementation.
     * HLL sketch is compressed using run length compression. */
    private static class RunLenHllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;
        private final int precision;

        RunLenHllValue(int precision) {
            this.precision = precision;
        }

        @Override
        public int precision() {
            return precision;
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
            valuesInBuffer = 0;
        }

        @Override
        public boolean next() {
            if (valuesInBuffer > 0) {
                valuesInBuffer--;
                return true;
            }
            if (dataInput.eof() == false) {
                valuesInBuffer = dataInput.readVInt() - 1;
                value = dataInput.readByte();
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            if (valuesInBuffer >= bytes) {
                valuesInBuffer -= bytes;
            } else {
                int valuesLeft = valuesInBuffer;
                valuesInBuffer = 0;
                next();
                skip(bytes - valuesLeft - 1);
            }
        }
    }

    /** re-usable {@link AbstractHyperLogLog.RunLenIterator} implementation.
     * HLL sketch is compressed using custom compression. Negative values indicate repeated values. */
    private static class CustomHllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;
        private final int precision;

        CustomHllValue(int precision) {
            this.precision = precision;
        }

        @Override
        public int precision() {
            return precision;
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
            valuesInBuffer = 0;
        }

        @Override
        public boolean next() {
            if (valuesInBuffer > 0) {
                valuesInBuffer--;
                return true;
            }
            if (dataInput.eof() == false) {
                byte b = dataInput.readByte();
                if (b < 0) {
                    valuesInBuffer = -b - 1;
                    value = dataInput.readByte();
                } else {
                    value = b;
                }
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            if (valuesInBuffer >= bytes) {
                valuesInBuffer -= bytes;
            } else {
                int valuesLeft = valuesInBuffer;
                valuesInBuffer = 0;
                next();
                skip(bytes - valuesLeft - 1);
            }
        }
    }

    /** re-usable {@link AbstractLinearCounting.EncodedHashesIterator} implementation.
     *  LC sketch is compressed using delta encoding.
     */
    private static class LcValue implements AbstractLinearCounting.EncodedHashesIterator {
        private int size;
        private int value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private final int precision;

        LcValue(int precision) {
            this.precision = precision;
        }

        /** reset the value for the LC sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            size = this.dataInput.readVInt();
            isExhausted = false;
            value = 0;
        }

        @Override
        public int precision() {
            return precision;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (dataInput.eof() == false) {
                if (value == 0) {
                    value = dataInput.readInt();
                } else {
                    value += dataInput.readVLong();
                }
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public int value() {
            if (isExhausted) {
                throw new IllegalArgumentException("Linear Counting sketch already exhausted");
            }
            return value;
        }
    }
}
