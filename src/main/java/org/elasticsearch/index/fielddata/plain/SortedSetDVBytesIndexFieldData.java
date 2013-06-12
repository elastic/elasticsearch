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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper.Names;

public class SortedSetDVBytesIndexFieldData extends DocValuesIndexFieldData implements IndexFieldData.WithOrdinals<SortedSetDVBytesAtomicFieldData> {

    public SortedSetDVBytesIndexFieldData(Index index, Names fieldNames) {
        super(index, fieldNames);
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, SortMode sortMode) {
        return new BytesRefFieldComparatorSource((IndexFieldData<?>) this, missingValue, sortMode);
    }

    @Override
    public SortedSetDVBytesAtomicFieldData load(AtomicReaderContext context) {
        final SortedSetDVBytesAtomicFieldData atomicFieldData = new SortedSetDVBytesAtomicFieldData(context.reader(), fieldNames.indexName());
        updateMaxUniqueValueCount(atomicFieldData.getNumberUniqueValues());
        return atomicFieldData;
    }

    @Override
    public SortedSetDVBytesAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }
}
