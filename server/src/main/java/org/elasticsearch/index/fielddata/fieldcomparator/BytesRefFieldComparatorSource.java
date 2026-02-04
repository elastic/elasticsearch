/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.TermOrdValComparator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

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

    protected void setScorer(LeafReaderContext context, Scorable scorer) {}

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final boolean sortMissingLast = sortMissingLast(missingValue) ^ reversed;
        final BytesRef missingBytes = (BytesRef) missingObject(missingValue, reversed);
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            return new TermOrdValComparator(numHits, null, sortMissingLast, reversed, Pruning.NONE) {

                @Override
                protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
                    final SortedSetDocValues values = ((IndexOrdinalsFieldData) indexFieldData).load(context).getOrdinalsValues();
                    final SortedDocValues selectedValues;
                    if (nested == null) {
                        selectedValues = sortMode.select(values);
                    } else {
                        final BitSet rootDocs = nested.rootDocs(context);
                        final DocIdSetIterator innerDocs = nested.innerDocs(context);
                        final int maxChildren = nested.getNestedSort() != null
                            ? nested.getNestedSort().getMaxChildren()
                            : Integer.MAX_VALUE;
                        selectedValues = sortMode.select(values, rootDocs, innerDocs, maxChildren);
                    }
                    if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                        return selectedValues;
                    } else {
                        return new ReplaceMissing(selectedValues, missingBytes);
                    }
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
                    selectedValues = sortMode.select(values, missingBytes, rootDocs, innerDocs, maxChildren);
                }
                return selectedValues;
            }

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                LeafFieldComparator leafComparator = super.getLeafComparator(context);
                // TopFieldCollector interacts with inter-segment concurrency by creating a FieldValueHitQueue per slice, each one with a
                // specific instance of the FieldComparator. This ensures sequential execution across LeafFieldComparators returned by
                // the same parent FieldComparator. That allows for effectively sharing the same instance of leaf comparator, like in this
                // case in the Lucene code. That's fine dealing with sorting by field, but not when using script sorting, because we then
                // need to set to Scorer to the specific leaf comparator, to make the _score variable available in sort scripts. The
                // setScorer call happens concurrently across slices and needs to target the specific leaf context that is being searched.
                return new LeafFieldComparator() {
                    @Override
                    public void setBottom(int slot) throws IOException {
                        leafComparator.setBottom(slot);
                    }

                    @Override
                    public int compareBottom(int doc) throws IOException {
                        return leafComparator.compareBottom(doc);
                    }

                    @Override
                    public int compareTop(int doc) throws IOException {
                        return leafComparator.compareTop(doc);
                    }

                    @Override
                    public void copy(int slot, int doc) throws IOException {
                        leafComparator.copy(slot, doc);
                    }

                    @Override
                    public void setScorer(Scorable scorer) {
                        // this ensures that the scorer is set for the specific leaf comparator
                        // corresponding to the leaf context we are scoring
                        BytesRefFieldComparatorSource.this.setScorer(context, scorer);
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
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
                substituteOrd = -sub - 1;
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
                return in.lookupOrd(ord - 1);
            } else {
                return in.lookupOrd(ord);
            }
        }

        // we let termsenum etc fall back to the default implementation
    }
}
