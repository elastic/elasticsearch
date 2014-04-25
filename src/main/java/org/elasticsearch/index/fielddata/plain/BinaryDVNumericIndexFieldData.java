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

package org.elasticsearch.index.fielddata.plain;

import com.google.common.base.Preconditions;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.index.mapper.FieldMapper.Names;

import java.io.IOException;

public class BinaryDVNumericIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData<BinaryDVNumericAtomicFieldData> {

    private final NumericType numericType;

    public BinaryDVNumericIndexFieldData(Index index, Names fieldNames, NumericType numericType, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
        Preconditions.checkArgument(numericType != null, "numericType must be non-null");
        this.numericType = numericType;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(final Object missingValue, final MultiValueMode sortMode) {
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

    @Override
    public BinaryDVNumericAtomicFieldData load(AtomicReaderContext context) {
        try {
            return new BinaryDVNumericAtomicFieldData(context.reader(), context.reader().getBinaryDocValues(fieldNames.indexName()), numericType);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public BinaryDVNumericAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

}
