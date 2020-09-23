/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValue;

import java.io.IOException;

/** read/write HLL doc values */
class HLLDocValuesBuilder {

    // compression modes for doc values
    private static final byte FIXED_LENGTH = 0;
    private static final byte RUN_LENGTH = 1;
    private static final byte CUSTOM = 2;

    final InternalFixedLengthHllValue fixedValue;
    final InternalRunLenHllValue runLenValue;
    final InternalCustomHllValue customValue;

    HLLDocValuesBuilder() {
        fixedValue = new InternalFixedLengthHllValue();
        runLenValue = new InternalRunLenHllValue();
        customValue = new InternalCustomHllValue();
    }

    static void writeCompressed(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        // chose the compression to use
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

    protected HllValue decode(ByteArrayDataInput dataInput) throws IOException {
        byte mode = dataInput.readByte();
        switch (mode) {
            case FIXED_LENGTH:
                fixedValue.reset(dataInput);
                return fixedValue;
            case RUN_LENGTH:
                runLenValue.reset(dataInput);
                return runLenValue;
            case CUSTOM:
                customValue.reset(dataInput);
                return customValue;
            default:
                throw new IOException("Unknown compression mode: " + mode);
        }
    }

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed as fixed length array */
    private static class InternalFixedLengthHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;

        InternalFixedLengthHllValue() {
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

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed using run length compression. */
    private static class InternalRunLenHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;

        InternalRunLenHllValue() {
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

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed using custom compression. Negative values
     * indicate repeated values. */
    private static class InternalCustomHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;

        InternalCustomHllValue() {
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
}
