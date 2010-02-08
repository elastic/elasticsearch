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

package org.elasticsearch.search.facets;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContextFacets;
import org.elasticsearch.util.lucene.Lucene;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class FacetsPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("facets", new FacetsParseElement());
    }

    @Override public void execute(SearchContext context) throws ElasticSearchException {
        if (context.facets() == null) {
            return;
        }
        if (context.queryResult().facets() != null) {
            // no need to compute the facets twice, they should be computed on a per conext basis
            return;
        }

        SearchContextFacets contextFacets = context.facets();

        List<Facet> facets = Lists.newArrayListWithCapacity(2);
        if (contextFacets.queryFacets() != null) {
            for (SearchContextFacets.QueryFacet queryFacet : contextFacets.queryFacets()) {
                Filter facetFilter = new QueryWrapperFilter(queryFacet.query());
                facetFilter = context.filterCache().cache(facetFilter);
                long count;
                if (contextFacets.queryType() == SearchContextFacets.QueryExecutionType.COLLECT) {
                    count = executeQueryCollectorCount(context, queryFacet, facetFilter);
                } else if (contextFacets.queryType() == SearchContextFacets.QueryExecutionType.IDSET) {
                    count = executeQueryIdSetCount(context, queryFacet, facetFilter);
                } else {
                    throw new ElasticSearchIllegalStateException("No matching for type [" + contextFacets.queryType() + "]");
                }
                facets.add(new CountFacet(queryFacet.name(), count));
            }
        }

        context.queryResult().facets(new Facets(facets));
    }

    private long executeQueryIdSetCount(SearchContext context, SearchContextFacets.QueryFacet queryFacet, Filter facetFilter) {
        try {
            DocIdSet filterDocIdSet = facetFilter.getDocIdSet(context.searcher().getIndexReader());
            return OpenBitSet.intersectionCount(context.searcher().docIdSet(), (OpenBitSet) filterDocIdSet);
        } catch (IOException e) {
            throw new FacetPhaseExecutionException(queryFacet.name(), "Failed to bitset facets for query [" + queryFacet.query() + "]", e);
        }
    }

    private long executeQueryCollectorCount(SearchContext context, SearchContextFacets.QueryFacet queryFacet, Filter facetFilter) {
        Lucene.CountCollector countCollector = new Lucene.CountCollector(-1.0f);
        try {
            context.searcher().search(context.query(), facetFilter, countCollector);
        } catch (IOException e) {
            throw new FacetPhaseExecutionException(queryFacet.name(), "Failed to collect facets for query [" + queryFacet.query() + "]", e);
        }
        return countCollector.count();
    }
}
