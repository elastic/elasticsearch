/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.field.data.bytes;

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;

/**
 *
 */
public class MultiValueByteFieldData extends ByteFieldData {

    private static final int VALUE_CACHE_SIZE = 10;

    private ThreadLocal<ThreadLocals.CleanableValue<double[][]>> doublesValuesCache = new ThreadLocal<ThreadLocals.CleanableValue<double[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<double[][]> initialValue() {
            double[][] value = new double[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new double[i];
            }
            return new ThreadLocals.CleanableValue<double[][]>(value);
        }
    };

    private ThreadLocal<ThreadLocals.CleanableValue<byte[][]>> valuesCache = new ThreadLocal<ThreadLocals.CleanableValue<byte[][]>>() {
        @Override
        protected ThreadLocals.CleanableValue<byte[][]> initialValue() {
            byte[][] value = new byte[VALUE_CACHE_SIZE][];
            for (int i = 0; i < value.length; i++) {
                value[i] = new byte[i];
            }
            return new ThreadLocals.CleanableValue<byte[][]>(value);
        }
    };

    // order with value 0 indicates no value
    private final int[][] ordinals;

    public MultiValueByteFieldData(String fieldName, int[][] ordinals, byte[] values) {
        super(fieldName, values);
        this.ordinals = ordinals;
    }

    @Override
    protected long computeSizeInBytes() {
        long size = super.computeSizeInBytes();
        size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level array
        for (int[] ordinal : ordinals) {
            size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
        }
        return size;
    }

    @Override
    public boolean multiValued() {
        return true;
    }

    @Override
    public boolean hasValue(int docId) {
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachValueInDoc(int docId, StringValueInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onMissing(docId);
                }
                break;
            }
            proc.onValue(docId, Byte.toString(values[loc]));
        }
    }

    @Override
    public void forEachValueInDoc(int docId, DoubleValueInDocProc proc) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc == 0) {
                break;
            }
            proc.onValue(docId, values[loc]);
        }
    }

    @Override
    public void forEachValueInDoc(int docId, LongValueInDocProc proc) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc == 0) {
                break;
            }
            proc.onValue(docId, values[loc]);
        }
    }

    @Override
    public void forEachValueInDoc(int docId, MissingDoubleValueInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onMissing(docId);
                }
                break;
            }
            proc.onValue(docId, values[loc]);
        }
    }

    @Override
    public void forEachValueInDoc(int docId, MissingLongValueInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onMissing(docId);
                }
                break;
            }
            proc.onValue(docId, values[loc]);
        }
    }

    @Override
    public void forEachValueInDoc(int docId, ValueInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onMissing(docId);
                }
                break;
            }
            proc.onValue(docId, values[loc]);
        }
    }

    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
        for (int i = 0; i < ordinals.length; i++) {
            int loc = ordinals[i][docId];
            if (loc == 0) {
                if (i == 0) {
                    proc.onOrdinal(docId, 0);
                }
                break;
            }
            proc.onOrdinal(docId, loc);
        }
    }

    @Override
    public double[] doubleValues(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] == 0) {
                break;
            }
            length++;
        }
        if (length == 0) {
            return DoubleFieldData.EMPTY_DOUBLE_ARRAY;
        }
        double[] doubles;
        if (length < VALUE_CACHE_SIZE) {
            doubles = doublesValuesCache.get().get()[length];
        } else {
            doubles = new double[length];
        }
        for (int i = 0; i < length; i++) {
            doubles[i] = values[ordinals[i][docId]];
        }
        return doubles;
    }

    @Override
    public byte value(int docId) {
        for (int[] ordinal : ordinals) {
            int loc = ordinal[docId];
            if (loc != 0) {
                return values[loc];
            }
        }
        return 0;
    }

    @Override
    public byte[] values(int docId) {
        int length = 0;
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] == 0) {
                break;
            }
            length++;
        }
        if (length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        byte[] bytes;
        if (length < VALUE_CACHE_SIZE) {
            bytes = valuesCache.get().get()[length];
        } else {
            bytes = new byte[length];
        }
        for (int i = 0; i < length; i++) {
            bytes[i] = values[ordinals[i][docId]];
        }
        return bytes;
    }
}