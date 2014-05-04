/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.AbstractAtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.LongValues;

final class BinaryDVNumericAtomicFieldData extends AbstractAtomicNumericFieldData {

    private final AtomicReader reader;
    private final BinaryDocValues values;
    private final NumericType numericType;

    BinaryDVNumericAtomicFieldData(AtomicReader reader, BinaryDocValues values, NumericType numericType) {
        super(numericType.isFloatingPoint());
        this.reader = reader;
        this.values = values == null ? DocValues.EMPTY_BINARY : values;
        this.numericType = numericType;
    }

    @Override
    public LongValues getLongValues() {
        if (numericType.isFloatingPoint()) {
            return LongValues.asLongValues(getDoubleValues());
        }
        return new LongValues(true) {

            final BytesRef bytes = new BytesRef();
            final ByteArrayDataInput in = new ByteArrayDataInput();
            long[] longs = new long[8];
            int i = Integer.MAX_VALUE;
            int valueCount = 0;

            @Override
            public int setDocument(int docId) {
                values.get(docId, bytes);
                in.reset(bytes.bytes, bytes.offset, bytes.length);
                if (!in.eof()) {
                    // first value uses vLong on top of zig-zag encoding, then deltas are encoded using vLong
                    long previousValue = longs[0] = ByteUtils.zigZagDecode(ByteUtils.readVLong(in));
                    valueCount = 1;
                    while (!in.eof()) {
                        longs = ArrayUtil.grow(longs, valueCount + 1);
                        previousValue = longs[valueCount++] = previousValue + ByteUtils.readVLong(in);
                    }
                } else {
                    valueCount = 0;
                }
                i = 0;
                return valueCount;
            }

            @Override
            public long nextValue() {
                assert i < valueCount;
                return longs[i++];
            }

        };
    }

    @Override
    public DoubleValues getDoubleValues() {
        if (!numericType.isFloatingPoint()) {
            return DoubleValues.asDoubleValues(getLongValues());
        }
        switch (numericType) {
        case FLOAT:
            return new DoubleValues(true) {

                final BytesRef bytes = new BytesRef();
                int i = Integer.MAX_VALUE;
                int valueCount = 0;

                @Override
                public int setDocument(int docId) {
                    values.get(docId, bytes);
                    assert bytes.length % 4 == 0;
                    i = 0;
                    return valueCount = bytes.length / 4;
                }

                @Override
                public double nextValue() {
                    assert i < valueCount;
                    return ByteUtils.readFloatLE(bytes.bytes, bytes.offset + i++ * 4);
                }

            };
        case DOUBLE:
            return new DoubleValues(true) {

                final BytesRef bytes = new BytesRef();
                int i = Integer.MAX_VALUE;
                int valueCount = 0;

                @Override
                public int setDocument(int docId) {
                    values.get(docId, bytes);
                    assert bytes.length % 8 == 0;
                    i = 0;
                    return valueCount = bytes.length / 8;
                }

                @Override
                public double nextValue() {
                    assert i < valueCount;
                    return ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + i++ * 8);
                }

            };
        default:
            throw new AssertionError();
        }
    }

    @Override
    public boolean isMultiValued() {
        return true; // no way to know
    }

    @Override
    public long getNumberUniqueValues() {
        return Long.MAX_VALUE; // no clue
    }

    @Override
    public long getMemorySizeInBytes() {
        return -1; // Lucene doesn't expose it
    }

    @Override
    public void close() {
        // no-op
    }

}
