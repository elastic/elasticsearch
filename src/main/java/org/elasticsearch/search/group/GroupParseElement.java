/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.group;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

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
 *
 *
 */
public class GroupParseElement implements SearchParseElement {

    private final GroupProcessors groupProcessors;

    @Inject
    public GroupParseElement(GroupProcessors groupProcessors) {
        this.groupProcessors = groupProcessors;
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;

        GroupCollector groupCollector = null;

        String topLevelFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                GroupCollector group = null;
                String scope = ContextIndexSearcher.Scopes.MAIN;
                String facetFieldName = null;
                Filter filter = null;
                boolean cacheFilter = true;
                String nestedPath = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        facetFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("facet_filter".equals(facetFieldName) || "facetFilter".equals(facetFieldName)) {
                            filter = context.queryParserService().parseInnerFilter(parser);
                        } else {
                            GroupProcessor groupProcessor = groupProcessors.processor(facetFieldName);
                            if (groupProcessor == null) {
                                throw new SearchParseException(context, "No facet type found for [" + facetFieldName + "]");
                            }
                            group = groupProcessor.parse(topLevelFieldName, parser, context);
                        }
                    } else if (token.isValue()) {
                        if ("global".equals(facetFieldName)) {
                            if (parser.booleanValue()) {
                                scope = ContextIndexSearcher.Scopes.GLOBAL;
                            }
                        } else if ("scope".equals(facetFieldName) || "_scope".equals(facetFieldName)) {
                            scope = parser.text();
                        } else if ("cache_filter".equals(facetFieldName) || "cacheFilter".equals(facetFieldName)) {
                            cacheFilter = parser.booleanValue();
                        } else if ("nested".equals(facetFieldName)) {
                            nestedPath = parser.text();
                        }
                    }
                }
                if (filter != null) {
                    if (cacheFilter) {
                        filter = context.filterCache().cache(filter);
                    }
                    group.setFilter(filter);
                }

                if (nestedPath != null) {
                    // its a nested facet, wrap the collector with a facet one...
                    MapperService.SmartNameObjectMapper mapper = context.smartNameObjectMapper(nestedPath);
                    if (mapper == null) {
                        throw new SearchParseException(context, "facet nested path [" + nestedPath + "] not found");
                    }
                    ObjectMapper objectMapper = mapper.mapper();
                    if (objectMapper == null) {
                        throw new SearchParseException(context, "facet nested path [" + nestedPath + "] not found");
                    }
                    if (!objectMapper.nested().isNested()) {
                        throw new SearchParseException(context, "facet nested path [" + nestedPath + "] is not nested");
                    }
//                    facet = new NestedChildrenCollector(facet, context.filterCache().cache(NonNestedDocsFilter.INSTANCE), context.filterCache().cache(objectMapper.nestedTypeFilter()));
                }

                if (group == null) {
                    throw new SearchParseException(context, "no facet type found for facet named [" + topLevelFieldName + "]");
                }

                groupCollector = group;
                context.searcher().addCollector(scope, group);
            }
        }

        context.groups(new SearchContextGroup(groupCollector));
    }
}
