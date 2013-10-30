/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FilterBytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 * Sorts by field's natural Term sort order.  All
 * comparisons are done using BytesRef.compareTo, which is
 * slow for medium to large result sets but possibly
 * very fast for very small results sets.
 */
public final class BytesRefValComparator extends NestedWrappableComparator<BytesRef> {

    private final IndexFieldData<?> indexFieldData;
    private final SortMode sortMode;
    private final BytesRef missingValue;

    private final BytesRef[] values;
    private BytesRef bottom;
    private BytesValues docTerms;

    BytesRefValComparator(IndexFieldData<?> indexFieldData, int numHits, SortMode sortMode, BytesRef missingValue) {
        this.sortMode = sortMode;
        values = new BytesRef[numHits];
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        return compareValues(val1, val2);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        int length = docTerms.setDocument(doc); // safes one hasValue lookup
        BytesRef val2 = length == 0 ? missingValue : docTerms.nextValue();
        return compareValues(bottom, val2);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        int length = docTerms.setDocument(doc); // safes one hasValue lookup
        if (length == 0) {
            values[slot] = missingValue;
        } else {
            if (values[slot] == null || values[slot] == missingValue) {
                values[slot] = new BytesRef();
            }
            values[slot].copyBytes(docTerms.nextValue());
        }
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = indexFieldData.load(context).getBytesValues(false);
        if (docTerms.isMultiValued()) {
            docTerms = new MultiValuedBytesWrapper(docTerms, sortMode);
        }
        return this;
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
        final int length = docTerms.setDocument(doc); // safes one hasValue lookup
        return  (length == 0 ? missingValue : docTerms.nextValue()).compareTo(value);
    }

    private static final class MultiValuedBytesWrapper extends FilterBytesValues {

        private final SortMode sortMode;
        private int numValues;

        public MultiValuedBytesWrapper(BytesValues delegate, SortMode sortMode) {
            super(delegate);
            this.sortMode = sortMode;
        }

        public int setDocument(int docId) {
            // either 0 or 1
            return Math.min(1, (numValues = delegate.setDocument(docId)));
        }

        public BytesRef nextValue() {
            BytesRef currentVal = delegate.nextValue();
            // We MUST allocate a new byte[] since relevantVal might have been filled by reference by a PagedBytes instance
            // meaning that the BytesRef.bytes are shared and shouldn't be overwritten. We can't use the bytes of the iterator
            // either because they will be overwritten by subsequent calls in the current thread
            scratch.bytes = new byte[ArrayUtil.oversize(currentVal.length, RamUsageEstimator.NUM_BYTES_BYTE)];
            scratch.offset = 0;
            scratch.length = currentVal.length;
            System.arraycopy(currentVal.bytes, currentVal.offset, scratch.bytes, 0, currentVal.length);
            for (int i = 1; i < numValues; i++) {
                currentVal = delegate.nextValue();
                if (sortMode == SortMode.MAX) {
                    if (currentVal.compareTo(scratch) > 0) {
                        scratch.grow(currentVal.length);
                        scratch.length = currentVal.length;
                        System.arraycopy(currentVal.bytes, currentVal.offset, scratch.bytes, 0, currentVal.length);
                    }
                } else {
                    if (currentVal.compareTo(scratch) < 0) {
                        scratch.grow(currentVal.length);
                        scratch.length = currentVal.length;
                        System.arraycopy(currentVal.bytes, currentVal.offset, scratch.bytes, 0, currentVal.length);
                    }
                }
            }
            return scratch;
        }

    }

    @Override
    public void missing(int slot) {
        values[slot] = missingValue;
    }

    @Override
    public int compareBottomMissing() {
        return compareValues(bottom, missingValue);
    }

}
