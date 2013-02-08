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
import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

abstract class DoubleValuesComparatorBase<T extends Number> extends FieldComparator<T> {

    protected final IndexNumericFieldData<?>indexFieldData;
    protected final double missingValue;
    protected double bottom;
    protected DoubleValues readerValues;
    private final boolean reversed;

    public DoubleValuesComparatorBase(IndexNumericFieldData<?> indexFieldData, double missingValue, boolean reversed) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.reversed = reversed;
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
            readerValues = new MultiValuedBytesWrapper(readerValues, reversed);
        }
        return this;
    }
    
    static final int compare(double left, double right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }
    
    static final class MultiValuedBytesWrapper extends DoubleValues.FilteredDoubleValues {

        private final boolean reversed;
        
        public MultiValuedBytesWrapper(DoubleValues delegate, boolean reversed) {
            super(delegate);
            this.reversed = reversed;
        }

        @Override
        public double getValueMissing(int docId, double missing) {
            DoubleValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return missing;
            }

            double currentVal = iter.next();
            double relevantVal = currentVal;
            while (true) {
                int cmp = Double.compare(currentVal, relevantVal);
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
        }

    }


}
