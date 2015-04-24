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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class NumericDVIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData {

    public NumericDVIndexFieldData(Index index, Names fieldNames, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
    }

    @Override
    public AtomicLongFieldData load(LeafReaderContext context) {
        final LeafReader reader = context.reader();
        final String field = fieldNames.indexName();
        return new AtomicLongFieldData(0) {
            @Override
            public SortedNumericDocValues getLongValues() {
                try {
                    final NumericDocValues values = DocValues.getNumeric(reader, field);
                    final Bits docsWithField = DocValues.getDocsWithField(reader, field);
                    return DocValues.singleton(values, docsWithField);
                } catch (IOException e) {
                    throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
                }
            }
            
            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }
        };

    }

    @Override
    public AtomicLongFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType getNumericType() {
        return NumericType.LONG;
    }
}
