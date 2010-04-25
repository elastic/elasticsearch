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

import org.elasticsearch.util.gcommon.collect.Lists;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.query.json.JsonIndexQueryParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.Booleans;

import java.util.List;

/**
 * <pre>
 * facets : {
 *  query_execution : "collect|idset",
 *  facet1: {
 *      query : { ... },
 *      global : false
 *  }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class FacetsParseElement implements SearchParseElement {

    @Override public void parse(JsonParser jp, SearchContext context) throws Exception {
        JsonToken token;
        SearchContextFacets.QueryExecutionType queryExecutionType = SearchContextFacets.QueryExecutionType.COLLECT;
        List<SearchContextFacets.QueryFacet> queryFacets = null;
        String topLevelFieldName = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                topLevelFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_STRING) {
                if ("query_execution".equals(topLevelFieldName) || "queryExecution".equals(topLevelFieldName)) {
                    String text = jp.getText();
                    if ("collect".equals(text)) {
                        queryExecutionType = SearchContextFacets.QueryExecutionType.COLLECT;
                    } else if ("idset".equals(text)) {
                        queryExecutionType = SearchContextFacets.QueryExecutionType.IDSET;
                    } else {
                        throw new SearchParseException(context, "Unsupported query type [" + text + "]");
                    }
                }
            } else if (token == JsonToken.START_OBJECT) {
                SearchContextFacets.Facet facet = null;
                boolean global = false;
                String facetFieldName = null;
                while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        facetFieldName = jp.getCurrentName();
                    } else if (token == JsonToken.START_OBJECT) {
                        if ("query".equals(facetFieldName)) {
                            JsonIndexQueryParser indexQueryParser = (JsonIndexQueryParser) context.queryParser();
                            Query facetQuery = indexQueryParser.parse(jp);
                            facet = new SearchContextFacets.QueryFacet(topLevelFieldName, facetQuery);
                            if (queryFacets == null) {
                                queryFacets = Lists.newArrayListWithCapacity(2);
                            }
                            queryFacets.add((SearchContextFacets.QueryFacet) facet);
                        }
                    } else if (token == JsonToken.VALUE_TRUE) {
                        if ("global".equals(facetFieldName)) {
                            global = true;
                        }
                    } else if (token == JsonToken.VALUE_NUMBER_INT) {
                        global = jp.getIntValue() != 0;
                    } else if (token == JsonToken.VALUE_STRING) {
                        global = Booleans.parseBoolean(jp.getText(), global);
                    }
                }
                if (facet == null) {
                    throw new SearchParseException(context, "No facet type found for [" + topLevelFieldName + "]");
                }
                facet.global(global);
            }
        }

        if (queryExecutionType == SearchContextFacets.QueryExecutionType.IDSET) {
            // if we are using doc id sets, we need to enable the fact that we accumelate it
            context.searcher().enabledDocIdSet();
        }

        context.facets(new SearchContextFacets(queryExecutionType, queryFacets));
    }
}
