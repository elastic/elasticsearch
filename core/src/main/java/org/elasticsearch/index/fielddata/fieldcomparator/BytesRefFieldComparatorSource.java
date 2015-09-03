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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
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
    private final MultiValueMode sortMode;
    private final Object missingValue;
    private final Nested nested;

    public BytesRefFieldComparatorSource(IndexFieldData<?> indexFieldData, Object missingValue, MultiValueMode sortMode, Nested nested) {
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        this.missingValue = missingValue;
        this.nested = nested;
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

    protected void setScorer(Scorer scorer) {}

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldNames().indexName());

        final boolean sortMissingLast = sortMissingLast(missingValue) ^ reversed;
        final BytesRef missingBytes = (BytesRef) missingObject(missingValue, reversed);
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            return new FieldComparator.TermOrdValComparator(numHits, null, sortMissingLast) {
                
                @Override
                protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
                    final RandomAccessOrds values = ((IndexOrdinalsFieldData) indexFieldData).load(context).getOrdinalsValues();
                    final SortedDocValues selectedValues;
                    if (nested == null) {
                        selectedValues = sortMode.select(values);
                    } else {
                        final BitSet rootDocs = nested.rootDocs(context);
                        final DocIdSet innerDocs = nested.innerDocs(context);
                        selectedValues = sortMode.select(values, rootDocs, innerDocs);
                    }
                    if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                        return selectedValues;
                    } else {
                        return new ReplaceMissing(selectedValues, missingBytes);
                    }
                }
                
                @Override
                public void setScorer(Scorer scorer) {
                    BytesRefFieldComparatorSource.this.setScorer(scorer);
                }

            };
        }

        final BytesRef nullPlaceHolder = new BytesRef();
        final BytesRef nonNullMissingBytes = missingBytes == null ? nullPlaceHolder : missingBytes;
        return new FieldComparator.TermValComparator(numHits, null, sortMissingLast) {

            @Override
            protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedBinaryDocValues values = getValues(context);
                final BinaryDocValues selectedValues;
                if (nested == null) {
                    selectedValues = sortMode.select(values, nonNullMissingBytes);
                } else {
                    final BitSet rootDocs = nested.rootDocs(context);
                    final DocIdSet innerDocs = nested.innerDocs(context);
                    selectedValues = sortMode.select(values, nonNullMissingBytes, rootDocs, innerDocs, context.reader().maxDoc());
                }
                return selectedValues;
            }

            @Override
            protected Bits getDocsWithField(LeafReaderContext context, String field) throws IOException {
                return new Bits.MatchAllBits(context.reader().maxDoc());
            }

            @Override
            protected boolean isNull(int doc, BytesRef term) {
                return term == nullPlaceHolder;
            }

            @Override
            public void setScorer(Scorer scorer) {
                BytesRefFieldComparatorSource.this.setScorer(scorer);
            }

        };
    }
    
    /** 
     * A view of a SortedDocValues where missing values 
     * are replaced with the specified term  
     */
    // TODO: move this out if we need it for other reasons
    static class ReplaceMissing extends SortedDocValues {
        final SortedDocValues in;
        final int substituteOrd;
        final BytesRef substituteTerm;
        final boolean exists;
        
        ReplaceMissing(SortedDocValues in, BytesRef term) {
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
        public int getOrd(int docID) {
            int ord = in.getOrd(docID);
            if (ord < 0) {
                return substituteOrd;
            } else if (exists == false && ord >= substituteOrd) {
                return ord + 1;
            } else {
                return ord;
            }
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
        public BytesRef lookupOrd(int ord) {
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
