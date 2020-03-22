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
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

/** Returns an implementation based on paged bytes which doesn't implement WithOrdinals in order to visit different paths in the code,
 *  eg. BytesRefFieldComparatorSource makes decisions based on whether the field data implements WithOrdinals. */
public class NoOrdinalsStringFieldDataTests extends PagedBytesStringFieldDataTests {

    public static IndexFieldData<LeafFieldData> hideOrdinals(final IndexFieldData<?> in) {
        return new IndexFieldData<LeafFieldData>() {

            @Override
            public Index index() {
                return in.index();
            }

            @Override
            public String getFieldName() {
                return in.getFieldName();
            }

            @Override
            public LeafFieldData load(LeafReaderContext context) {
                return in.load(context);
            }

            @Override
            public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
                return in.loadDirect(context);
            }

            @Override
            public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
                return new SortField(getFieldName(), source, reverse);
            }

            @Override
            public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
                    SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                in.clear();
            }

        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public IndexFieldData<LeafFieldData> getForField(String fieldName) {
        return hideOrdinals(super.getForField(fieldName));
    }

    @Override
    public void testTermsEnum() throws Exception {
        assumeTrue("We can't test this, since the returned IFD instance doesn't implement IndexFieldData.WithOrdinals", false);
    }
}
