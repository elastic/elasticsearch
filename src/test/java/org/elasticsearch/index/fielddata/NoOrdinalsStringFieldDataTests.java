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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper.Names;

/** Returns an implementation based on paged bytes which doesn't implement WithOrdinals in order to visit different paths in the code,
 *  eg. BytesRefFieldComparatorSource makes decisions based on whether the field data implements WithOrdinals. */
public class NoOrdinalsStringFieldDataTests extends PagedBytesStringFieldDataTests {

    @SuppressWarnings("unchecked")
    @Override
    public IndexFieldData<AtomicFieldData<ScriptDocValues>> getForField(String fieldName) {
        final IndexFieldData<?> in = super.getForField(fieldName);
        return new IndexFieldData<AtomicFieldData<ScriptDocValues>>() {

            @Override
            public Index index() {
                return in.index();
            }

            @Override
            public Names getFieldNames() {
                return in.getFieldNames();
            }

            @Override
            public boolean valuesOrdered() {
                return in.valuesOrdered();
            }

            @Override
            public AtomicFieldData<ScriptDocValues> load(AtomicReaderContext context) {
                return in.load(context);
            }

            @Override
            public AtomicFieldData<ScriptDocValues> loadDirect(AtomicReaderContext context) throws Exception {
                return in.loadDirect(context);
            }

            @Override
            public XFieldComparatorSource comparatorSource(Object missingValue, SortMode sortMode) {
                return new BytesRefFieldComparatorSource(this, missingValue, sortMode);
            }

            @Override
            public void clear() {
                in.clear();
            }

            @Override
            public void clear(IndexReader reader) {
                in.clear(reader);
            }

            @Override
            public long getHighestNumberOfSeenUniqueValues() {
                return in.getHighestNumberOfSeenUniqueValues();
            }

        };
    }

}
