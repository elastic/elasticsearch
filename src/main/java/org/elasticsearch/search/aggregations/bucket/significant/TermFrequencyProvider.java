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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class TermFrequencyProvider implements Releasable {

    
    private FilterableTermsEnum termsEnum;
    private String indexedFieldName;
    private Filter filter;
    private FieldMapper mapper;
    private boolean useCaching = false;

    public TermFrequencyProvider(String indexedFieldName, Filter filter, FieldMapper mapper) {
        this.indexedFieldName = indexedFieldName;
        this.filter = filter;
        this.mapper = mapper;
    }

    protected boolean isUseCaching() {
        return useCaching;
    }

    protected void setUseCaching(boolean useCaching) {
        this.useCaching = useCaching;
    }

    /**
     * Creates the TermsEnum (if not already created) and must be called before any calls to getBackgroundFrequency
     * @param context The aggregation context 
     * @return The number of documents in the index (after an optional filter might have been applied)
     */
    public long prepareBackground(AggregationContext context) {
        if (termsEnum != null) {
            // already prepared - return 
            return termsEnum.getNumDocs();
        }
        SearchContext searchContext = context.searchContext();
        IndexReader reader = searchContext.searcher().getIndexReader();
        try {
            if (!useCaching) {
                // Setup a termsEnum for sole use by one aggregator
                termsEnum = new FilterableTermsEnum(reader, indexedFieldName, DocsEnum.FLAG_NONE, filter);
            } else {
                // When we have > 1 agg we have possibility of duplicate term frequency lookups 
                // and so use a TermsEnum that caches results of all term lookups
                termsEnum = new FreqTermsEnum(reader, indexedFieldName, true, false, filter, searchContext.bigArrays());
            }
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build terms enumeration", e);
        }
        return termsEnum.getNumDocs();
    }

    public long getBackgroundFrequency(BytesRef termBytes) {
        assert termsEnum != null; // having failed to find a field in the index we don't expect any calls for frequencies
        long result = 0;
        try {
            if (termsEnum.seekExact(termBytes)) {
                result = termsEnum.docFreq();
            }
        } catch (IOException e) {
            throw new ElasticsearchException("IOException loading background document frequency info", e);
        }
        return result;
    }


    public long getBackgroundFrequency(long term) {
        BytesRef indexedVal = mapper.indexedValueForSearch(term);
        return getBackgroundFrequency(indexedVal);
    }

    @Override
    public void close() throws ElasticsearchException {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).close();
            }
        } finally {
            termsEnum = null;
        }
    }    
}
