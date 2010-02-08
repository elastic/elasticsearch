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

import com.google.common.collect.Lists;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.query.json.JsonIndexQueryParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContextFacets;

import java.util.List;

/**
 * <pre>
 * facets : {
 *  queryExecution : "collect|idset",
 *  facet1: {
 *      query : { ... }
 *  }
 * }
 * </pre>
 *
 * @author kimchy (Shay Banon)
 */
public class FacetsParseElement implements SearchParseElement {

    @Override public void parse(JsonParser jp, SearchContext context) throws Exception {
        JsonToken token;
        SearchContextFacets.QueryExecutionType queryExecutionType = SearchContextFacets.QueryExecutionType.COLLECT;
        List<SearchContextFacets.QueryFacet> queryFacets = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                String topLevelFieldName = jp.getCurrentName();

                if ("queryExecution".equals(topLevelFieldName)) {
                    jp.nextToken(); // move to value
                    String text = jp.getText();
                    if ("collect".equals(text)) {
                        queryExecutionType = SearchContextFacets.QueryExecutionType.COLLECT;
                    } else if ("idset".equals(text)) {
                        queryExecutionType = SearchContextFacets.QueryExecutionType.IDSET;
                    } else {
                        throw new SearchParseException("Unsupported query type [" + text + "]");
                    }
                } else {

                    jp.nextToken(); // move to START_OBJECT

                    jp.nextToken(); // move to FIELD_NAME
                    String facetType = jp.getCurrentName();

                    if ("query".equals(facetType)) {
                        JsonIndexQueryParser indexQueryParser = (JsonIndexQueryParser) context.queryParser();
                        Query facetQuery = indexQueryParser.parse(jp, context.source());

                        if (queryFacets == null) {
                            queryFacets = Lists.newArrayListWithCapacity(2);
                        }
                        queryFacets.add(new SearchContextFacets.QueryFacet(topLevelFieldName, facetQuery));
                    } else {
                        throw new SearchParseException("Unsupported facet type [" + facetType + "] for facet name [" + topLevelFieldName + "]");
                    }
                    jp.nextToken();
                }
            }
        }

        if (queryExecutionType == SearchContextFacets.QueryExecutionType.IDSET) {
            // if we are using doc id sets, we need to enable the fact that we accumelate it
            context.searcher().enabledDocIdSet();
        }

        context.facets(new SearchContextFacets(queryExecutionType, queryFacets));
    }
}
