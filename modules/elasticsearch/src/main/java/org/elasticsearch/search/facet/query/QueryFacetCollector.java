/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.OptimizeGlobalFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class QueryFacetCollector extends AbstractFacetCollector implements OptimizeGlobalFacetCollector {

    private final Query query;

    private final Filter filter;

    private DocSet docSet;

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

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        docSet = DocSets.convert(reader, filter.getDocIdSet(reader));
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (docSet.get(doc)) {
            count++;
        }
    }

    @Override public void optimizedGlobalExecution(SearchContext searchContext) throws IOException {
        Query query = this.query;
        if (super.filter != null) {
            query = new FilteredQuery(query, super.filter);
        }
        if (searchContext.types().length > 0) {
            query = new FilteredQuery(query, searchContext.filterCache().cache(searchContext.mapperService().typesFilter(searchContext.types())));
        }
        TotalHitCountCollector collector = new TotalHitCountCollector();
        searchContext.searcher().search(query, collector);
        count = collector.getTotalHits();
    }

    @Override public Facet facet() {
        return new InternalQueryFacet(facetName, count);
    }

    /**
     * If its a filtered query with a match all, then we just need the inner filter.
     */
    private Filter extractFilterIfApplicable(Query query) {
        if (query instanceof FilteredQuery) {
            FilteredQuery fQuery = (FilteredQuery) query;
            if (Queries.isMatchAllQuery(fQuery.getQuery())) {
                return fQuery.getFilter();
            }
        } else if (query instanceof DeletionAwareConstantScoreQuery) {
            return ((DeletionAwareConstantScoreQuery) query).getFilter();
        } else if (query instanceof ConstantScoreQuery) {
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            if (constantScoreQuery.getFilter() != null) {
                return constantScoreQuery.getFilter();
            }
        }
        return null;
    }
}
