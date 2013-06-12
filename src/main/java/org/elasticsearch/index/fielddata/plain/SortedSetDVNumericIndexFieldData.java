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

package org.elasticsearch.index.fielddata.plain;

import com.google.common.base.Preconditions;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.*;
import org.elasticsearch.index.mapper.FieldMapper.Names;

import java.io.IOException;

public class SortedSetDVNumericIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData<SortedSetDVNumericAtomicFieldData> {

    private final NumericType numericType;

    public SortedSetDVNumericIndexFieldData(Index index, Names fieldNames, NumericType numericType) {
        super(index, fieldNames);
        Preconditions.checkArgument(numericType != null, "numericType must be non-null");
        this.numericType = numericType;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(final Object missingValue, final SortMode sortMode) {
        if (sortMode == SortMode.SUM || sortMode == SortMode.AVG) {
            // sort based on an aggregation, we can't use ordinals here so it may be slowish
            switch (numericType) {
            case FLOAT:
                return new FloatValuesComparatorSource(this, missingValue, sortMode);
            case DOUBLE:
                return new DoubleValuesComparatorSource(this, missingValue, sortMode);
            default:
                assert !numericType.isFloatingPoint();
                return new LongValuesComparatorSource(this, missingValue, sortMode);
            }
        }
        assert sortMode == SortMode.MIN || sortMode == SortMode.MAX;
        // Otherwise (MIN/MAX), use ordinal-based comparison -> fast
        final IndexFieldData.WithOrdinals<?> bytesIndexFieldData = new SortedSetDVBytesIndexFieldData(index, fieldNames);
        return new XFieldComparatorSource() {

            @Override
            public Type reducedType() {
                return numericType.sortFieldType();
            }

            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                assert fieldname.equals(bytesIndexFieldData.getFieldNames().indexName());

                final Number missingNumber = (Number) missingObject(missingValue, reversed);
                final BytesRef missingBytes = new BytesRef();
                numericType.toIndexForm(missingNumber, missingBytes);

                final BytesRefOrdValComparator in = new BytesRefOrdValComparator((IndexFieldData.WithOrdinals<?>) bytesIndexFieldData, numHits, sortMode, missingBytes);
                return new NumericFieldComparator(in, numericType);
            }

        };
    }

    private static class NumericFieldComparator extends NestedWrappableComparator<Number> {

        final NestedWrappableComparator<BytesRef> in;
        final NumericType numericType;
        final BytesRef spare;

        public NumericFieldComparator(NestedWrappableComparator<BytesRef> in, NumericType numericType) {
            this.in = in;
            this.numericType = numericType;
            spare = new BytesRef();
        }

        @Override
        public int compare(int slot1, int slot2) {
            return in.compare(slot1, slot2);
        }

        @Override
        public void setBottom(int slot) {
            in.setBottom(slot);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return in.compareBottom(doc);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            in.copy(slot, doc);
        }

        @Override
        public FieldComparator<Number> setNextReader(AtomicReaderContext context) throws IOException {
            return new NumericFieldComparator((NestedWrappableComparator<BytesRef>) in.setNextReader(context), numericType);
        }

        @Override
        public Number value(int slot) {
            final BytesRef value = in.value(slot);
            if (value == null) {
                return null;
            }
            return numericType.toNumber(value);
        }

        @Override
        public int compareDocToValue(int doc, Number value) throws IOException {
            if (value == null) {
                return in.compareDocToValue(doc, null);
            }
            numericType.toIndexForm(value, spare);
            return in.compareDocToValue(doc, spare);
        }

        @Override
        public void missing(int slot) {
            in.missing(slot);
        }

        @Override
        public int compareBottomMissing() {
            return in.compareBottomMissing();
        }

    }

    @Override
    public SortedSetDVNumericAtomicFieldData load(AtomicReaderContext context) {
        final SortedSetDVNumericAtomicFieldData atomicFieldData = new SortedSetDVNumericAtomicFieldData(context.reader(), fieldNames.indexName(), numericType);
        updateMaxUniqueValueCount(atomicFieldData.getNumberUniqueValues());
        return atomicFieldData;
    }

    @Override
    public SortedSetDVNumericAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

}
