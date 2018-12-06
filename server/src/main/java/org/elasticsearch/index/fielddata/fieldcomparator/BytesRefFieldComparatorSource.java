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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for string/binary values.
 */
public class BytesRefFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexFieldData<?> indexFieldData;

    public BytesRefFieldComparatorSource(IndexFieldData<?> indexFieldData, Object missingValue, MultiValueMode sortMode, Nested nested) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.STRING;
    }

    @Override
    public Object missingValue(boolean reversed) {
        if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
            if (sortMissingLast(missingValue) ^ reversed) {
                return SortField.STRING_LAST;
            } else {
                return SortField.STRING_FIRST;
            }
        }
        // otherwise we fill missing values ourselves
        return null;
    }

    protected SortedBinaryDocValues getValues(LeafReaderContext context) throws IOException {
        return indexFieldData.load(context).getBytesValues();
    }

    protected void setScorer(Scorable scorer) {}

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final boolean sortMissingLast = sortMissingLast(missingValue) ^ reversed;
        final BytesRef missingBytes = (BytesRef) missingObject(missingValue, reversed);
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            return new FieldComparator.TermOrdValComparator(numHits, null, sortMissingLast) {

                @Override
                protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
                    final SortedSetDocValues values = ((IndexOrdinalsFieldData) indexFieldData).load(context).getOrdinalsValues();
                    final SortedDocValues selectedValues;
                    if (nested == null) {
                        selectedValues = sortMode.select(values);
                    } else {
                        final BitSet rootDocs = nested.rootDocs(context);
                        final DocIdSetIterator innerDocs = nested.innerDocs(context);
                        final int maxChildren = nested.getNestedSort() != null ?
                            nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
                        selectedValues = sortMode.select(values, rootDocs, innerDocs, maxChildren);
                    }
                    if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                        return selectedValues;
                    } else {
                        return new ReplaceMissing(selectedValues, missingBytes);
                    }
                }

                @Override
                public void setScorer(Scorable scorer) {
                    BytesRefFieldComparatorSource.this.setScorer(scorer);
                }

            };
        }

        return new FieldComparator.TermValComparator(numHits, null, sortMissingLast) {

            @Override
            protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedBinaryDocValues values = getValues(context);
                final BinaryDocValues selectedValues;
                if (nested == null) {
                    selectedValues = sortMode.select(values, missingBytes);
                } else {
                    final BitSet rootDocs = nested.rootDocs(context);
                    final DocIdSetIterator innerDocs = nested.innerDocs(context);
                    final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
                    selectedValues = sortMode.select(values, missingBytes, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
                }
                return selectedValues;
            }

            @Override
            public void setScorer(Scorable scorer) {
                BytesRefFieldComparatorSource.this.setScorer(scorer);
            }

        };
    }

    /**
     * A view of a SortedDocValues where missing values
     * are replaced with the specified term
     */
    // TODO: move this out if we need it for other reasons
    static class ReplaceMissing extends AbstractSortedDocValues {
        final SortedDocValues in;
        final int substituteOrd;
        final BytesRef substituteTerm;
        final boolean exists;
        boolean hasValue;

        ReplaceMissing(SortedDocValues in, BytesRef term) throws IOException {
            this.in = in;
            this.substituteTerm = term;
            int sub = in.lookupTerm(term);
            if (sub < 0) {
                substituteOrd = -sub-1;
                exists = false;
            } else {
                substituteOrd = sub;
                exists = true;
            }
        }

        @Override
        public int ordValue() throws IOException {
            if (hasValue == false) {
                return substituteOrd;
            }
            int ord = in.ordValue();
            if (exists == false && ord >= substituteOrd) {
                return ord + 1;
            } else {
                return ord;
            }
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            hasValue = in.advanceExact(target);
            return true;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int getValueCount() {
            if (exists) {
                return in.getValueCount();
            } else {
                return in.getValueCount() + 1;
            }
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            if (ord == substituteOrd) {
                return substituteTerm;
            } else if (exists == false && ord > substituteOrd) {
                return in.lookupOrd(ord-1);
            } else {
                return in.lookupOrd(ord);
            }
        }

        // we let termsenum etc fall back to the default implementation
    }
}
