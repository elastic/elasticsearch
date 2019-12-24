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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.function.Function;

/**
 * Comparator source for long values.
 */
public class LongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;
    private final Function<SortedNumericDocValues, SortedNumericDocValues> converter;

    public LongValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue,
                                      MultiValueMode sortMode, Nested nested) {
        this(indexFieldData, missingValue, sortMode, nested, null);
    }

    public LongValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue,
                                      MultiValueMode sortMode, Nested nested,
                                      Function<SortedNumericDocValues, SortedNumericDocValues> converter) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
        this.converter = converter;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    private SortedNumericDocValues loadDocValues(LeafReaderContext context) {
        final AtomicNumericFieldData data = indexFieldData.load(context);
        SortedNumericDocValues values;
        if (data instanceof SortedNumericDVIndexFieldData.NanoSecondFieldData) {
            values = ((SortedNumericDVIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
        } else {
            values = data.getLongValues();
        }
        return converter != null ? converter.apply(values) : values;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final Long dMissingValue = (Long) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new FieldComparator.LongComparator(numHits, null, null) {
            @Override
            protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedNumericDocValues values = loadDocValues(context);
                final NumericDocValues selectedValues;
                if (nested == null) {
                    selectedValues = FieldData.replaceMissing(sortMode.select(values), dMissingValue);
                } else {
                    final BitSet rootDocs = nested.rootDocs(context);
                    final DocIdSetIterator innerDocs = nested.innerDocs(context);
                    final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
                    selectedValues = sortMode.select(values, dMissingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
                }
                return selectedValues;
            }

        };
    }
}
