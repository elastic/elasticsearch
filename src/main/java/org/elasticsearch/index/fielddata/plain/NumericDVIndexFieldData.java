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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.index.mapper.FieldMapper.Names;

public class NumericDVIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData<NumericDVAtomicFieldData> {

    public NumericDVIndexFieldData(Index index, Names fieldNames, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public NumericDVAtomicFieldData load(AtomicReaderContext context) {
        return new NumericDVAtomicFieldData(context.reader(), fieldNames.indexName());
    }

    @Override
    public NumericDVAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode) {
        return new LongValuesComparatorSource(this, missingValue, sortMode);
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType getNumericType() {
        return NumericType.LONG;
    }
}
