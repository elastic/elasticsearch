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

package org.elasticsearch.search.facet.query;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.OptimizeGlobalFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class QueryFacetCollector extends AbstractFacetCollector implements OptimizeGlobalFacetCollector {

    private final Query query;

    private final Filter filter;

    private Bits bits;

    private int count = 0;

    public QueryFacetCollector(String facetName, Query query, FilterCache filterCache) {
        super(facetName);
        this.query = query;
        Filter possibleFilter = extractFilterIfApplicable(query);
        if (possibleFilter != null) {
            this.filter = possibleFilter;
        } else {
            this.filter = new QueryWrapperFilter(query);
        }
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        bits = DocIdSets.toSafeBits(context.reader(), filter.getDocIdSet(context, context.reader().getLiveDocs()));
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        if (bits.get(doc)) {
            count++;
        }
    }

    @Override
    public void optimizedGlobalExecution(SearchContext searchContext) throws IOException {
        Query query = this.query;
        if (super.filter != null) {
            query = new XFilteredQuery(query, super.filter);
        }
        Filter searchFilter = searchContext.mapperService().searchFilter(searchContext.types());
        if (searchFilter != null) {
            query = new XFilteredQuery(query, searchContext.filterCache().cache(searchFilter));
        }
        TotalHitCountCollector collector = new TotalHitCountCollector();
        searchContext.searcher().search(query, collector);
        count = collector.getTotalHits();
    }

    @Override
    public Facet facet() {
        return new InternalQueryFacet(facetName, count);
    }

    /**
     * If its a filtered query with a match all, then we just need the inner filter.
     */
    private Filter extractFilterIfApplicable(Query query) {
        if (query instanceof XFilteredQuery) {
            XFilteredQuery fQuery = (XFilteredQuery) query;
            if (Queries.isConstantMatchAllQuery(fQuery.getQuery())) {
                return fQuery.getFilter();
            }
        } else if (query instanceof XConstantScoreQuery) {
            return ((XConstantScoreQuery) query).getFilter();
        } else if (query instanceof ConstantScoreQuery) {
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            if (constantScoreQuery.getFilter() != null) {
                return constantScoreQuery.getFilter();
            }
        }
        return null;
    }
}
