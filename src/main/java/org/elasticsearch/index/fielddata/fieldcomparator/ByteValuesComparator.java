/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.ByteValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.util.ByteArrayRef;

import java.io.IOException;

/**
 */
public class ByteValuesComparator extends FieldComparator<Byte> {

    private final IndexNumericFieldData indexFieldData;
    private final byte missingValue;
    private final boolean reversed;

    private final byte[] values;
    private byte bottom;
    private ByteValues readerValues;

    public ByteValuesComparator(IndexNumericFieldData indexFieldData, byte missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
        this.values = new byte[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final byte v1 = values[slot1];
        final byte v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void setBottom(int slot) {
        this.bottom = values[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        byte v2 = readerValues.getValueMissing(doc, missingValue);

        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        values[slot] = readerValues.getValueMissing(doc, missingValue);
    }

    @Override
    public FieldComparator<Byte> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getByteValues();
        if (readerValues.isMultiValued()) {
            readerValues = new MultiValuedBytesWrapper(readerValues, reversed);
        }
        return this;
    }

    @Override
    public Byte value(int slot) {
        return Byte.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Byte valueObj) throws IOException {
        final byte value = valueObj.byteValue();
        byte docValue = readerValues.getValueMissing(doc, missingValue);
        if (docValue < value) {
            return -1;
        } else if (docValue > value) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class FilteredByteValues implements ByteValues {

        protected final ByteValues delegate;

        public FilteredByteValues(ByteValues delegate) {
            this.delegate = delegate;
        }

        public boolean isMultiValued() {
            return delegate.isMultiValued();
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public byte getValue(int docId) {
            return delegate.getValue(docId);
        }

        public byte getValueMissing(int docId, byte missingValue) {
            return delegate.getValueMissing(docId, missingValue);
        }

        public ByteArrayRef getValues(int docId) {
            return delegate.getValues(docId);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }

        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            delegate.forEachValueInDoc(docId, proc);
        }
    }

    private static final class MultiValuedBytesWrapper extends FilteredByteValues {

        private final boolean reversed;

        public MultiValuedBytesWrapper(ByteValues delegate, boolean reversed) {
            super(delegate);
            this.reversed = reversed;
        }

        @Override
        public byte getValueMissing(int docId, byte missing) {
            ByteValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return missing;
            }

            byte currentVal = iter.next();
            byte relevantVal = currentVal;
            while (true) {
                if (reversed) {
                    if (currentVal > relevantVal) {
                        relevantVal = currentVal;
                    }
                } else {
                    if (currentVal < relevantVal) {
                        relevantVal = currentVal;
                    }
                }
                if (!iter.hasNext()) {
                    break;
                }
                currentVal = iter.next();
            }
            return relevantVal;
        }

    }

}
