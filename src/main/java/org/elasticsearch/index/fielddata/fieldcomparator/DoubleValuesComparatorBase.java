package org.elasticsearch.index.fielddata.fieldcomparator;
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.FilterDoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

import java.io.IOException;

abstract class DoubleValuesComparatorBase<T extends Number> extends NumberComparatorBase<T> {

    protected final IndexNumericFieldData<?> indexFieldData;
    protected final double missingValue;
    protected double bottom;
    protected DoubleValues readerValues;
    private final SortMode sortMode;

    public DoubleValuesComparatorBase(IndexNumericFieldData<?> indexFieldData, double missingValue, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public final int compareBottom(int doc) throws IOException {
        final double v2 = readerValues.getValueMissing(doc, missingValue);
        return compare(bottom, v2);
    }

    @Override
    public final int compareDocToValue(int doc, T valueObj) throws IOException {
        final double value = valueObj.doubleValue();
        final double docValue = readerValues.getValueMissing(doc, missingValue);
        return compare(docValue, value);
    }

    @Override
    public final FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getDoubleValues();
        if (readerValues.isMultiValued()) {
            readerValues = new MultiValueWrapper(readerValues, sortMode);
        }
        return this;
    }

    @Override
    public int compareBottomMissing() {
        return compare(bottom, missingValue);
    }

    static final int compare(double left, double right) {
        return Double.compare(left, right);
    }

    static final class MultiValueWrapper extends FilterDoubleValues {

        private final SortMode sortMode;

        public MultiValueWrapper(DoubleValues delegate, SortMode sortMode) {
            super(delegate);
            this.sortMode = sortMode;
        }

        @Override
        public double getValueMissing(int docId, double missing) {
            int numValues = delegate.setDocument(docId);
            if (numValues == 0) {
                return missing;
            }
            double relevantVal = sortMode.startDouble();
            for (int i = 0; i < numValues; i++) {
                relevantVal = sortMode.apply(relevantVal, delegate.nextValue());
            }
            return sortMode.reduce(relevantVal, numValues);
        }

    }


}
