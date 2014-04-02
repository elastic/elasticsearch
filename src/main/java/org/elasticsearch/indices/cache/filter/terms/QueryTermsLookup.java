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

package org.elasticsearch.indices.cache.filter.terms;

import com.google.common.base.Joiner;
import org.apache.lucene.search.Filter;
import org.elasticsearch.action.terms.ResponseTerms;
import org.elasticsearch.action.terms.TermsByQueryAction;
import org.elasticsearch.action.terms.TermsByQueryRequest;
import org.elasticsearch.action.terms.TermsByQueryResponse;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.search.BloomFieldDataTermsFilter;
import org.elasticsearch.index.search.FieldDataTermsFilter;

/**
 * A {@link TermsLookup} implementation that gathers the filter terms from the specified field of documents matching
 * the specified filter.
 */
public class QueryTermsLookup extends TermsLookup {

    private final TermsByQueryRequest request;
    private final IndexFieldData fieldData;
    private ResponseTerms terms;

    public QueryTermsLookup(TermsByQueryRequest request, IndexFieldData fieldData) {
        super();
        this.request = request;
        this.fieldData = fieldData;
    }

    /**
     * Executes the lookup query and gathers the terms.  It generates a {@link FieldDataTermsFilter} that uses the
     * fielddata to lookup and filter values.
     *
     * @return the lookup filter
     */
    @Override
    public Filter getFilter() {
        TermsByQueryResponse termsByQueryResp = client.execute(TermsByQueryAction.INSTANCE, request).actionGet();
        terms = termsByQueryResp.getResponseTerms();

        if (terms.size() == 0) {
            return null;
        }

        Filter filter;
        switch (terms.getType()) {
            case BLOOM:
                filter = new BloomFieldDataTermsFilter(fieldData, (BloomFilter) terms.getTerms());
                break;
            case LONGS:
                filter = FieldDataTermsFilter.newLongs((IndexNumericFieldData) fieldData, (LongHash) terms.getTerms());
                break;
            case DOUBLES:
                filter = FieldDataTermsFilter.newDoubles((IndexNumericFieldData) fieldData, (LongHash) terms.getTerms());
                break;
            default:
                filter = FieldDataTermsFilter.newBytes(fieldData, (BytesRefHash) terms.getTerms());
        }


        return filter;
    }

    /**
     * String representation of the query lookup.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        // to_field/index1,index2,.../type1,type2,.../from_field/filter(filter_bytes_hash)
        Joiner joiner = Joiner.on(",");
        StringBuilder repr = new StringBuilder(fieldData.getFieldNames().indexName())
                .append(":").append(joiner.join(request.indices())).append("/")
                .append(joiner.join(request.types())).append("/").append(request.field()).append("/");

        if (request.filterSource() != null) {
            repr.append("filter(");
            try {
                repr.append(request.filterSource().toBytesArray().hashCode());
            } finally {
                repr.append(")");
            }
        }

        if (request.useBloomFilter()) {
            repr.append("/bloom");
        }

        return repr.toString();
    }

    /**
     * Deletages to {@link org.elasticsearch.action.terms.ResponseTerms#getSizeInBytes()}
     *
     * @return the estimated size of the filter in bytes.
     */
    @Override
    public long estimateSizeInBytes() {
        return terms.getSizeInBytes();
    }
}
