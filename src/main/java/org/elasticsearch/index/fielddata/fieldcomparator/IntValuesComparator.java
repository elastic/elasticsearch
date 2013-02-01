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
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IntValues;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

import java.io.IOException;

/**
 */
public class IntValuesComparator extends FieldComparator<Integer> {

    private final IndexNumericFieldData indexFieldData;
    private final int missingValue;
    private final boolean reversed;

    private final int[] values;
    private int bottom;
    private IntValues readerValues;

    public IntValuesComparator(IndexNumericFieldData indexFieldData, int missingValue, int numHits, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
        this.values = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final int v1 = values[slot1];
        final int v2 = values[slot2];
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
        int v2 = readerValues.getValueMissing(doc, missingValue);

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
    public FieldComparator<Integer> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getIntValues();
        if (readerValues.isMultiValued()) {
            readerValues = new MultiValuedBytesWrapper(readerValues, reversed);
        }
        return this;
    }

    @Override
    public Integer value(int slot) {
        return Integer.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Integer valueObj) throws IOException {
        final int value = valueObj.intValue();
        int docValue = readerValues.getValueMissing(doc, missingValue);
        if (docValue < value) {
            return -1;
        } else if (docValue > value) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class FilteredByteValues implements IntValues {

        protected final IntValues delegate;

        public FilteredByteValues(IntValues delegate) {
            this.delegate = delegate;
        }

        public boolean isMultiValued() {
            return delegate.isMultiValued();
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public int getValue(int docId) {
            return delegate.getValue(docId);
        }

        public int getValueMissing(int docId, int missingValue) {
            return delegate.getValueMissing(docId, missingValue);
        }

        public IntArrayRef getValues(int docId) {
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

        public MultiValuedBytesWrapper(IntValues delegate, boolean reversed) {
            super(delegate);
            this.reversed = reversed;
        }

        @Override
        public int getValueMissing(int docId, int missing) {
            IntValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return missing;
            }

            int currentVal = iter.next();
            int relevantVal = currentVal;
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
