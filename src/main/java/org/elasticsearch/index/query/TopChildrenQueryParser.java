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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.index.search.child.TopChildrenQuery;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class TopChildrenQueryParser implements QueryParser {

    public static final String NAME = "top_children";

    @Inject
    public TopChildrenQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreType scoreType = ScoreType.MAX;
        int factor = 5;
        int incrementalFactor = 2;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    queryFound = true;
                    // TODO we need to set the type, but, `query` can come before `type`... (see HasChildFilterParser)
                    // since we switch types, make sure we change the context
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(childType == null ? null : new String[]{childType});
                    try {
                        query = parseContext.parseInnerQuery();
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[top_children] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("_scope".equals(currentFieldName)) {
                    throw new QueryParsingException(parseContext.index(), "the [_scope] support in [top_children] query has been removed, use a filter as a facet_filter in the relevant global facet");
                } else if ("score".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("factor".equals(currentFieldName)) {
                    factor = parser.intValue();
                } else if ("incremental_factor".equals(currentFieldName) || "incrementalFactor".equals(currentFieldName)) {
                    incrementalFactor = parser.intValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[top_children] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[top_children] requires 'query' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[top_children] requires 'type' field");
        }

        if (query == null) {
            return null;
        }

        if ("delete_by_query".equals(SearchContext.current().source())) {
            throw new QueryParsingException(parseContext.index(), "[top_children] unsupported in delete_by_query api");
        }

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "No mapping for for type [" + childType + "]");
        }
        if (!childDocMapper.parentFieldMapper().active()) {
            throw new QueryParsingException(parseContext.index(), "Type [" + childType + "] does not have parent mapping");
        }
        String parentType = childDocMapper.parentFieldMapper().type();

        query.setBoost(boost);
        // wrap the query with type query
        query = new XFilteredQuery(query, parseContext.cacheFilter(childDocMapper.typeFilter(), null));
        TopChildrenQuery childQuery = new TopChildrenQuery(query, childType, parentType, scoreType, factor, incrementalFactor, parseContext.cacheRecycler());
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, childQuery);
        }
        return childQuery;
    }
}
