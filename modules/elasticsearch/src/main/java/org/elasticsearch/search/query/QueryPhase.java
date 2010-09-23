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

import org.apache.lucene.search.*;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facets.FacetsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

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
                .put("query_parser_name", new QueryParserNameParseElement())
                .put("queryParserName", new QueryParserNameParseElement())
                .put("indices_boost", new IndicesBoostParseElement())
                .put("indicesBoost", new IndicesBoostParseElement())
                .put("query", new QueryParseElement())
                .put("queryBinary", new QueryBinaryParseElement())
                .put("query_binary", new QueryBinaryParseElement())
                .put("sort", new SortParseElement())
                .putAll(facetsPhase.parseElements());
        return parseElements.build();
    }

    @Override public void preProcess(SearchContext context) {
        if (context.query() == null) {
            throw new SearchParseException(context, "No query specified in search request");
        }
        if (context.queryBoost() != 1.0f) {
            context.parsedQuery(new ParsedQuery(new FunctionScoreQuery(context.query(), new BoostScoreFunction(context.queryBoost())), context.parsedQuery()));
        }
        facetsPhase.preProcess(context);
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
                    if (docMapper == null) {
                        throw new TypeMissingException(new Index(searchContext.shardTarget().index()), type);
                    }
                    query = new FilteredQuery(query, searchContext.filterCache().cache(docMapper.typeFilter()));
                } else {
                    XBooleanFilter booleanFilter = new XBooleanFilter();
                    for (String type : searchContext.types()) {
                        DocumentMapper docMapper = searchContext.mapperService().documentMapper(type);
                        if (docMapper == null) {
                            throw new TypeMissingException(new Index(searchContext.shardTarget().index()), type);
                        }
                        booleanFilter.add(new FilterClause(searchContext.filterCache().cache(docMapper.typeFilter()), BooleanClause.Occur.SHOULD));
                    }
                    query = new FilteredQuery(query, booleanFilter);
                }
            }

            TopDocs topDocs;
            int numDocs = searchContext.from() + searchContext.size();
            if (numDocs == 0) {
                // if 0 was asked, change it to 1 since 0 is not allowed
                numDocs = 1;
            }
            boolean sort = false;
            // try and optimize for a case where the sorting is based on score, this is how we work by default!
            if (searchContext.sort() != null) {
                if (searchContext.sort().getSort().length > 1) {
                    sort = true;
                } else {
                    SortField sortField = searchContext.sort().getSort()[0];
                    if (sortField.getType() == SortField.SCORE && !sortField.getReverse()) {
                        sort = false;
                    } else {
                        sort = true;
                    }
                }
            }

            if (sort) {
                topDocs = searchContext.searcher().search(query, null, numDocs, searchContext.sort());
            } else {
                topDocs = searchContext.searcher().search(query, numDocs);
            }
            searchContext.queryResult().topDocs(topDocs);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }

        facetsPhase.execute(searchContext);
    }
}
