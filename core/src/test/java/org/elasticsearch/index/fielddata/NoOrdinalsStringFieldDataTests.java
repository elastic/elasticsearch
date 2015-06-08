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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.search.MultiValueMode;
import org.junit.Test;

/** Returns an implementation based on paged bytes which doesn't implement WithOrdinals in order to visit different paths in the code,
 *  eg. BytesRefFieldComparatorSource makes decisions based on whether the field data implements WithOrdinals. */
public class NoOrdinalsStringFieldDataTests extends PagedBytesStringFieldDataTests {

    public static IndexFieldData<AtomicFieldData> hideOrdinals(final IndexFieldData<?> in) {
        return new IndexFieldData<AtomicFieldData>() {

            @Override
            public Index index() {
                return in.index();
            }

            @Override
            public Names getFieldNames() {
                return in.getFieldNames();
            }

            @Override
            public FieldDataType getFieldDataType() {
                return in.getFieldDataType();
            }

            @Override
            public AtomicFieldData load(LeafReaderContext context) {
                return in.load(context);
            }

            @Override
            public AtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
                return in.loadDirect(context);
            }

            @Override
            public XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
                return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            }

            @Override
            public void clear() {
                in.clear();
            }

            @Override
            public void clear(IndexReader reader) {
                in.clear(reader);
            }

        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public IndexFieldData<AtomicFieldData> getForField(String fieldName) {
        return hideOrdinals(super.getForField(fieldName));
    }

    @Test
    @Override
    public void testTermsEnum() throws Exception {
        // We can't test this, since the returned IFD instance doesn't implement IndexFieldData.WithOrdinals
    }
}
