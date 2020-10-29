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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Comparator source for double values.
 */
public class DoubleValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public DoubleValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue, MultiValueMode sortMode,
            Nested nested) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.DOUBLE;
    }

    protected SortedNumericDoubleValues getValues(LeafReaderContext context) throws IOException {
        return indexFieldData.load(context).getDoubleValues();
    }

    private NumericDoubleValues getNumericDocValues(LeafReaderContext context, double missingValue) throws IOException {
        final SortedNumericDoubleValues values = getValues(context);
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        } else {
            final BitSet rootDocs = nested.rootDocs(context);
            final DocIdSetIterator innerDocs = nested.innerDocs(context);
            final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
            return sortMode.select(values, missingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
        }
    }

    protected void setScorer(Scorable scorer) {}

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final double dMissingValue = (Double) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new FieldComparator.DoubleComparator(numHits, null, null) {
            @Override
            protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                return DoubleValuesComparatorSource.this.getNumericDocValues(context, dMissingValue).getRawDoubleValues();
            }
            @Override
            public void setScorer(Scorable scorer) {
                DoubleValuesComparatorSource.this.setScorer(scorer);
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format,
            int bucketSize, BucketedSort.ExtraData extra) {
        return new BucketedSort.ForDoubles(bigArrays, sortOrder, format, bucketSize, extra) {
            private final double dMissingValue = (Double) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDoubleValues docValues = getNumericDocValues(ctx, dMissingValue);
                    private double docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.doubleValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected double docValue() {
                        return docValue;
                    }
                };
            }
        };
    }
}
