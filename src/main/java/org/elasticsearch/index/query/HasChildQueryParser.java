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

package org.elasticsearch.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.search.child.HasChildFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class HasChildQueryParser implements QueryParser {

    public static final String NAME = "has_child";

    @Inject
    public HasChildQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        float boost = 1.0f;
        String childType = null;
        String scope = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    // TODO we need to set the type, but, `query` can come before `type`... (see HasChildFilterParser)
                    // since we switch types, make sure we change the context
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(childType == null ? null : new String[]{childType});
                    try {
                        query = parseContext.parseInnerQuery();
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("_scope".equals(currentFieldName)) {
                    scope = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'query' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'type' field");
        }

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] No mapping for for type [" + childType + "]");
        }
        if (childDocMapper.parentFieldMapper() == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] does not have parent mapping");
        }
        String parentType = childDocMapper.parentFieldMapper().type();

        query.setBoost(boost);
        // wrap the query with type query
        query = new FilteredQuery(query, parseContext.cacheFilter(childDocMapper.typeFilter(), null));

        SearchContext searchContext = SearchContext.current();
        HasChildFilter childFilter = new HasChildFilter(query, scope, childType, parentType, searchContext);
        // we don't need DeletionAwareConstantScore, since we filter deleted parent docs in the filter
        ConstantScoreQuery childQuery = new ConstantScoreQuery(childFilter);
        childQuery.setBoost(boost);
        searchContext.addScopePhase(childFilter);
        return childQuery;
    }
}
