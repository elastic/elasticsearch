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

package org.elasticsearch.search.query;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.lucene.search.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facets.FacetsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.lucene.search.TermFilter;

import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryPhase implements SearchPhase {

    private final FacetsPhase facetsPhase;

    @Inject public QueryPhase(FacetsPhase facetsPhase) {
        this.facetsPhase = facetsPhase;
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("from", new FromParseElement()).put("size", new SizeParseElement())
                .put("queryParserName", new QueryParserNameParseElement())
                .put("query", new QueryParseElement())
                .put("sort", new SortParseElement())
                .putAll(facetsPhase.parseElements());
        return parseElements.build();
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        try {
            searchContext.queryResult().from(searchContext.from());
            searchContext.queryResult().size(searchContext.size());

            Query query = searchContext.query();
            if (searchContext.types().length > 0) {
                if (searchContext.types().length == 1) {
                    String type = searchContext.types()[0];
                    DocumentMapper docMapper = searchContext.mapperService().documentMapper(type);
                    Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
                    typeFilter = searchContext.filterCache().cache(typeFilter);
                    query = new FilteredQuery(query, typeFilter);
                } else {
                    BooleanFilter booleanFilter = new BooleanFilter();
                    for (String type : searchContext.types()) {
                        DocumentMapper docMapper = searchContext.mapperService().documentMapper(type);
                        Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
                        typeFilter = searchContext.filterCache().cache(typeFilter);
                        booleanFilter.add(new FilterClause(typeFilter, BooleanClause.Occur.SHOULD));
                    }
                    query = new FilteredQuery(query, booleanFilter);
                }
            }

            TopDocs topDocs;
            if (searchContext.sort() != null) {
                topDocs = searchContext.searcher().search(query, null, searchContext.from() + searchContext.size(), searchContext.sort());
            } else {
                topDocs = searchContext.searcher().search(query, searchContext.from() + searchContext.size());
            }
            searchContext.queryResult().topDocs(topDocs);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext);
        }

        facetsPhase.execute(searchContext);
    }
}
