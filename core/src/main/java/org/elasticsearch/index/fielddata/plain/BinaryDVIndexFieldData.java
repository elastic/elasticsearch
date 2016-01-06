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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.MultiValueMode;

public class BinaryDVIndexFieldData extends DocValuesIndexFieldData implements IndexFieldData<BinaryDVAtomicFieldData> {

    public BinaryDVIndexFieldData(Index index, String fieldName, FieldDataType fieldDataType) {
        super(index, fieldName, fieldDataType);
    }

    @Override
    public BinaryDVAtomicFieldData load(LeafReaderContext context) {
        return new BinaryDVAtomicFieldData(context.reader(), fieldName);
    }

    @Override
    public BinaryDVAtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }
}
