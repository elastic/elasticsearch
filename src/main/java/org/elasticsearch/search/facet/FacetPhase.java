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

package org.elasticsearch.search.facet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class FacetPhase implements SearchPhase {

    private final FacetParseElement facetParseElement;

    private final FacetBinaryParseElement facetBinaryParseElement;

    @Inject
    public FacetPhase(FacetParseElement facetParseElement, FacetBinaryParseElement facetBinaryParseElement) {
        this.facetParseElement = facetParseElement;
        this.facetBinaryParseElement = facetBinaryParseElement;
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("facets", facetParseElement, "facets_binary", facetBinaryParseElement, "facetsBinary", facetBinaryParseElement);
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.facets() != null && context.facets().queryCollectors() != null) {
            for (FacetCollector collector : context.facets().queryCollectors()) {
                context.searcher().addMainQueryCollector(collector);
            }
        }
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        if (context.facets() == null) {
            return;
        }
        if (context.queryResult().facets() != null) {
            // no need to compute the facets twice, they should be computed on a per context basis
            return;
        }

        // optimize global facet execution, based on filters (don't iterate over all docs), and check
        // if we have special facets that can be optimized for all execution, do it
        List<FacetCollector> collectors = context.facets().globalCollectors();

        if (collectors != null && !collectors.isEmpty()) {
            Map<Filter, List<Collector>> filtersByCollector = Maps.newHashMap();
            for (Collector collector : collectors) {
                if (collector instanceof OptimizeGlobalFacetCollector) {
                    try {
                        ((OptimizeGlobalFacetCollector) collector).optimizedGlobalExecution(context);
                    } catch (IOException e) {
                        throw new QueryPhaseExecutionException(context, "Failed to execute global facets", e);
                    }
                } else {
                    Filter filter = Queries.MATCH_ALL_FILTER;
                    if (collector instanceof AbstractFacetCollector) {
                        AbstractFacetCollector facetCollector = (AbstractFacetCollector) collector;
                        if (facetCollector.getFilter() != null) {
                            // we can clear the filter, since we are anyhow going to iterate over it
                            // so no need to double check it...
                            filter = facetCollector.getAndClearFilter();
                        }
                    }
                    List<Collector> list = filtersByCollector.get(filter);
                    if (list == null) {
                        list = new ArrayList<Collector>();
                        filtersByCollector.put(filter, list);
                    }
                    list.add(collector);
                }
            }
            // now, go and execute the filters->collector ones
            for (Map.Entry<Filter, List<Collector>> entry : filtersByCollector.entrySet()) {
                Filter filter = entry.getKey();
                Query query = new XConstantScoreQuery(filter);
                Filter searchFilter = context.mapperService().searchFilter(context.types());
                if (searchFilter != null) {
                    query = new XFilteredQuery(query, context.filterCache().cache(searchFilter));
                }
                try {
                    context.searcher().search(query, MultiCollector.wrap(entry.getValue().toArray(new Collector[entry.getValue().size()])));
                } catch (IOException e) {
                    throw new QueryPhaseExecutionException(context, "Failed to execute global facets", e);
                }
            }
        }

        SearchContextFacets contextFacets = context.facets();

        List<Facet> facets = Lists.newArrayListWithCapacity(2);
        if (contextFacets.queryCollectors() != null) {
            for (FacetCollector facetCollector : contextFacets.queryCollectors()) {
                facets.add(facetCollector.facet());
            }
        }
        if (contextFacets.globalCollectors() != null) {
            for (FacetCollector facetCollector : contextFacets.globalCollectors()) {
                facets.add(facetCollector.facet());
            }
        }
        context.queryResult().facets(new InternalFacets(facets));
    }
}
