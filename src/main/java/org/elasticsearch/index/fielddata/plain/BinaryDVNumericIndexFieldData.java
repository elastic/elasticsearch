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

import com.google.common.base.Preconditions;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class BinaryDVNumericIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData {

    private final NumericType numericType;

    public BinaryDVNumericIndexFieldData(Index index, Names fieldNames, NumericType numericType, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
        Preconditions.checkArgument(numericType != null, "numericType must be non-null");
        this.numericType = numericType;
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(final Object missingValue, final MultiValueMode sortMode, Nested nested) {
        switch (numericType) {
        case FLOAT:
            return new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
        case DOUBLE:
            return new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
        default:
            assert !numericType.isFloatingPoint();
            return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        }
    }

    @Override
    public AtomicNumericFieldData load(LeafReaderContext context) {
        try {
            final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldNames.indexName());
            if (numericType.isFloatingPoint()) {
                return new AtomicDoubleFieldData(-1) {

                    @Override
                    public SortedNumericDoubleValues getDoubleValues() {
                        switch (numericType) {
                        case FLOAT:
                            return new BinaryAsSortedNumericFloatValues(values);
                        case DOUBLE:
                            return new BinaryAsSortedNumericDoubleValues(values);
                        default:
                            throw new ElasticsearchIllegalArgumentException("" + numericType);
                        }
                    }
                    
                    @Override
                    public Collection<Accountable> getChildResources() {
                        return Collections.emptyList();
                    }

                };
            } else {
                return new AtomicLongFieldData(0) {

                    @Override
                    public SortedNumericDocValues getLongValues() {
                        return new BinaryAsSortedNumericDocValues(values);
                    }
                    
                    @Override
                    public Collection<Accountable> getChildResources() {
                        return Collections.emptyList();
                    }

                };
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public AtomicNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    private static class BinaryAsSortedNumericDocValues extends SortedNumericDocValues {

        private final BinaryDocValues values;
        private BytesRef bytes;
        private final ByteArrayDataInput in = new ByteArrayDataInput();
        private long[] longs = new long[1];
        private int count = 0;

        BinaryAsSortedNumericDocValues(BinaryDocValues values) {
            this.values = values;
        }

        @Override
        public void setDocument(int docId) {
            bytes = values.get(docId);
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            if (!in.eof()) {
                // first value uses vLong on top of zig-zag encoding, then deltas are encoded using vLong
                long previousValue = longs[0] = ByteUtils.zigZagDecode(ByteUtils.readVLong(in));
                count = 1;
                while (!in.eof()) {
                    longs = ArrayUtil.grow(longs, count + 1);
                    previousValue = longs[count++] = previousValue + ByteUtils.readVLong(in);
                }
            } else {
                count = 0;
            }
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public long valueAt(int index) {
            return longs[index];
        }

    }

    private static class BinaryAsSortedNumericDoubleValues extends SortedNumericDoubleValues {

        private final BinaryDocValues values;
        private BytesRef bytes;
        private int valueCount = 0;

        BinaryAsSortedNumericDoubleValues(BinaryDocValues values) {
            this.values = values;
        }

        @Override
        public void setDocument(int docId) {
            bytes = values.get(docId);
            assert bytes.length % 8 == 0;
            valueCount = bytes.length / 8;
        }

        @Override
        public int count() {
            return valueCount;
        }

        @Override
        public double valueAt(int index) {
            return ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + index * 8);
        }

    }

    private static class BinaryAsSortedNumericFloatValues extends SortedNumericDoubleValues {

        private final BinaryDocValues values;
        private BytesRef bytes;
        private int valueCount = 0;

        BinaryAsSortedNumericFloatValues(BinaryDocValues values) {
            this.values = values;
        }

        @Override
        public void setDocument(int docId) {
            bytes = values.get(docId);
            assert bytes.length % 4 == 0;
            valueCount = bytes.length / 4;
        }

        @Override
        public int count() {
            return valueCount;
        }

        @Override
        public double valueAt(int index) {
            return ByteUtils.readFloatLE(bytes.bytes, bytes.offset + index * 4);
        }

    }
}
