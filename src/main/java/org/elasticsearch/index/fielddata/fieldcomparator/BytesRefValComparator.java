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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;

import java.io.IOException;

/**
 * Sorts by field's natural Term sort order.  All
 * comparisons are done using BytesRef.compareTo, which is
 * slow for medium to large result sets but possibly
 * very fast for very small results sets.
 */
public final class BytesRefValComparator extends FieldComparator<BytesRef> {

    private final IndexFieldData indexFieldData;
    private final boolean reversed;

    private final BytesRef[] values;
    private BytesRef bottom;
    private BytesValues docTerms;

    BytesRefValComparator(IndexFieldData indexFieldData, int numHits, boolean reversed) {
        this.reversed = reversed;
        values = new BytesRef[numHits];
        this.indexFieldData = indexFieldData;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
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
    public int compareBottom(int doc) throws IOException {
        BytesRef val2 = docTerms.getValue(doc);
        if (bottom == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return bottom.compareTo(val2);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        if (values[slot] == null) {
            values[slot] = new BytesRef();
        }
        docTerms.getValueScratch(doc, values[slot]);
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = indexFieldData.load(context).getBytesValues();
        if (docTerms.isMultiValued()) {
            docTerms = new MultiValuedBytesWrapper(docTerms, reversed);
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
        return docTerms.getValue(doc).compareTo(value);
    }

    public static class FilteredByteValues implements BytesValues {

        protected final BytesValues delegate;

        public FilteredByteValues(BytesValues delegate) {
            this.delegate = delegate;
        }

        public boolean isMultiValued() {
            return delegate.isMultiValued();
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public BytesRef getValue(int docId) {
            return delegate.getValue(docId);
        }

        public BytesRef makeSafe(BytesRef bytes) {
            return delegate.makeSafe(bytes);
        }

        public BytesRef getValueScratch(int docId, BytesRef ret) {
            return delegate.getValueScratch(docId, ret);
        }

        public BytesRefArrayRef getValues(int docId) {
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

        public MultiValuedBytesWrapper(BytesValues delegate, boolean reversed) {
            super(delegate);
            this.reversed = reversed;
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef scratch) {
            BytesValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return null;
            }

            BytesRef currentVal = iter.next();
            BytesRef relevantVal = currentVal;
            while (true) {
                int cmp = currentVal.compareTo(relevantVal);
                if (reversed) {
                    if (cmp > 0) {
                        relevantVal = currentVal;
                    }
                } else {
                    if (cmp < 0) {
                        relevantVal = currentVal;
                    }
                }
                if (!iter.hasNext()) {
                    break;
                }
                currentVal = iter.next();
            }
            return relevantVal;
            /*if (reversed) {
                BytesRefArrayRef ref = readerValues.getValues(docId);
                if (ref.isEmpty()) {
                    return null;
                } else {
                    return ref.values[ref.end - 1]; // last element is the highest value.
                }
            } else {
                return readerValues.getValue(docId); // returns the lowest value
            }*/
        }

    }

}
