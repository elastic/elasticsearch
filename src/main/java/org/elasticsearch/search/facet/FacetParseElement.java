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
package org.elasticsearch.search.facet;

import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.facet.nested.NestedFacetExecutor;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * facets : {
 *  facet1: {
 *      query : { ... },
 *      global : false
 *  },
 *  facet2: {
 *      terms : {
 *          name : "myfield",
 *          size : 12
 *      },
 *      global : false
 *  }
 * }
 * </pre>
 */
public class FacetParseElement implements SearchParseElement {

    private final FacetParsers facetParsers;

    @Inject
    public FacetParseElement(FacetParsers facetParsers) {
        this.facetParsers = facetParsers;
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;

        List<SearchContextFacets.Entry> entries = new ArrayList<>();

        String facetName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                facetName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                FacetExecutor facetExecutor = null;
                boolean global = false;
                FacetExecutor.Mode defaultMainMode = null;
                FacetExecutor.Mode defaultGlobalMode = null;
                FacetExecutor.Mode mode = null;
                Filter filter = null;
                boolean cacheFilter = false;
                String nestedPath = null;

                String fieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("facet_filter".equals(fieldName) || "facetFilter".equals(fieldName)) {
                            ParsedFilter parsedFilter = context.queryParserService().parseInnerFilter(parser);
                            filter = parsedFilter == null ? null : parsedFilter.filter();
                        } else {
                            FacetParser facetParser = facetParsers.parser(fieldName);
                            if (facetParser == null) {
                                throw new SearchParseException(context, "No facet type found for [" + fieldName + "]");
                            }
                            facetExecutor = facetParser.parse(facetName, parser, context);
                            defaultMainMode = facetParser.defaultMainMode();
                            defaultGlobalMode = facetParser.defaultGlobalMode();
                        }
                    } else if (token.isValue()) {
                        if ("global".equals(fieldName)) {
                            global = parser.booleanValue();
                        } else if ("mode".equals(fieldName)) {
                            String modeAsText = parser.text();
                            if ("collector".equals(modeAsText)) {
                                mode = FacetExecutor.Mode.COLLECTOR;
                            } else if ("post".equals(modeAsText)) {
                                mode = FacetExecutor.Mode.POST;
                            } else {
                                throw new ElasticsearchIllegalArgumentException("failed to parse facet mode [" + modeAsText + "]");
                            }
                        } else if ("scope".equals(fieldName) || "_scope".equals(fieldName)) {
                            throw new SearchParseException(context, "the [scope] support in facets have been removed");
                        } else if ("cache_filter".equals(fieldName) || "cacheFilter".equals(fieldName)) {
                            cacheFilter = parser.booleanValue();
                        } else if ("nested".equals(fieldName)) {
                            nestedPath = parser.text();
                        }
                    }
                }

                if (filter != null) {
                    if (cacheFilter) {
                        filter = context.filterCache().cache(filter);
                    }
                }

                if (facetExecutor == null) {
                    throw new SearchParseException(context, "no facet type found for facet named [" + facetName + "]");
                }

                if (nestedPath != null) {
                    facetExecutor = new NestedFacetExecutor(facetExecutor, context, nestedPath);
                }

                if (mode == null) {
                    mode = global ? defaultGlobalMode : defaultMainMode;
                }
                entries.add(new SearchContextFacets.Entry(facetName, mode, facetExecutor, global, filter));
            }
        }

        context.facets(new SearchContextFacets(entries));
    }
}
