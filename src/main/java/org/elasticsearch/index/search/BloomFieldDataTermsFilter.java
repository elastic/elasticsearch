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

package org.elasticsearch.index.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 * Filters on non-numeric field values that might be found in a {@link BloomFilter}.
 */
public class BloomFieldDataTermsFilter extends FieldDataTermsFilter {

    final BloomFilter bloomFilter;
    Integer hashCode;

    /**
     * Constructor accepting a {@link BloomFilter}.
     */
    public BloomFieldDataTermsFilter(IndexFieldData fieldData, BloomFilter bloomFilter) {
        super(fieldData);
        this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || !(obj instanceof BloomFieldDataTermsFilter)) return false;

        BloomFieldDataTermsFilter that = (BloomFieldDataTermsFilter) obj;
        if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
        if (!bloomFilter.equals(that.bloomFilter)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            // calculate hashCode of each bloom filter
            int hash = fieldData.getFieldNames().indexName().hashCode();
            hash += bloomFilter != null ? bloomFilter.hashCode() : 0;

            hashCode = hash;
        }

        return hashCode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BloomFieldDataTermsFilter:");
        return sb
                .append(fieldData.getFieldNames().indexName())
                .append(":")
                .append(bloomFilter.toString()) // TODO: what to use here? hashCode maybe?
                .toString();
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        // make sure there are terms to filter on
        if (bloomFilter == null || bloomFilter.isEmpty()) return null;

        final BytesValues values = fieldData.load(context).getBytesValues(false); // load fielddata
        return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
            @Override
            protected boolean matchDoc(int doc) {
                final int numVals = values.setDocument(doc);
                for (int i = 0; i < numVals; i++) {
                    final BytesRef term = values.nextValue();
                    if (bloomFilter.mightContain(term)) {
                        return true;
                    }
                }

                return false;
            }
        };
    }
}
