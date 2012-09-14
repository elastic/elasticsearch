/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.index.search.child.HasParentFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

// Same parse logic HasParentQueryFilter, but also parses boost and wraps filter in constant score query
public class HasParentQueryParser implements QueryParser {

    public static final String NAME = "has_parent";

    @Inject
    public HasParentQueryParser() {
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
        String parentType = null;
        String executionType = "uid";
        String scope = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    // TODO handle `query` element before `type` element...
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(parentType == null ? null : new String[]{parentType});
                    try {
                        query = parseContext.parseInnerQuery();
                        queryFound = true;
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "parent_type".equals(currentFieldName) || "parentType".equals(currentFieldName)) {
                    parentType = parser.text();
                } else if ("_scope".equals(currentFieldName)) {
                    scope = parser.text();
                } else if ("execution_type".equals(currentFieldName) || "executionType".equals(currentFieldName)) { // This option is experimental and will most likely be removed.
                    executionType = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[parent] filter requires 'query' field");
        }
        if (query == null) {
            return null;
        }

        if (parentType == null) {
            throw new QueryParsingException(parseContext.index(), "[parent] filter requires 'parent_type' field");
        }

        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[parent] filter configured 'parent_type' [" + parentType + "] is not a valid type");
        }

        query.setBoost(boost);
        // wrap the query with type query
        query = new FilteredQuery(query, parseContext.cacheFilter(parentDocMapper.typeFilter(), null));
        SearchContext searchContext = SearchContext.current();
        HasParentFilter parentFilter = HasParentFilter.create(executionType, query, scope, parentType, searchContext);

        ConstantScoreQuery parentQuery = new ConstantScoreQuery(parentFilter);
        parentQuery.setBoost(boost);
        searchContext.addScopePhase(parentFilter);
        return parentQuery;
    }

}