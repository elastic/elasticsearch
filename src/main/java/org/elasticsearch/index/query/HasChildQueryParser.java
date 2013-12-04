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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.ChildrenQuery;
import org.elasticsearch.index.search.child.DeleteByQueryWrappingFilter;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
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

        Query innerQuery = null;
        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreType scoreType = null;
        int shortCircuitParentDocSet = 8192;
        String queryName = null;

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
                        innerQuery = parseContext.parseInnerQuery();
                        queryFound = true;
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("_scope".equals(currentFieldName)) {
                    throw new QueryParsingException(parseContext.index(), "the [_scope] support in [has_child] query has been removed, use a filter as a facet_filter in the relevant global facet");
                } else if ("score_type".equals(currentFieldName) || "scoreType".equals(currentFieldName)) {
                    String scoreTypeValue = parser.text();
                    if (!"none".equals(scoreTypeValue)) {
                        scoreType = ScoreType.fromString(scoreTypeValue);
                    }
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    String scoreModeValue = parser.text();
                    if (!"none".equals(scoreModeValue)) {
                        scoreType = ScoreType.fromString(scoreModeValue);
                    }
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("short_circuit_cutoff".equals(currentFieldName)) {
                    shortCircuitParentDocSet = parser.intValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'query' field");
        }
        if (innerQuery == null) {
            return null;
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'type' field");
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] No mapping for for type [" + childType + "]");
        }
        if (!childDocMapper.parentFieldMapper().active()) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] does not have parent mapping");
        }
        String parentType = childDocMapper.parentFieldMapper().type();
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);

        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] points to a non existent parent type [" + parentType + "]");
        }

        Filter nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.cacheFilter(NonNestedDocsFilter.INSTANCE, null);
        }

        // wrap the query with type query
        innerQuery = new XFilteredQuery(innerQuery, parseContext.cacheFilter(childDocMapper.typeFilter(), null));

        boolean deleteByQuery = "delete_by_query".equals(SearchContext.current().source());
        Query query;
        Filter parentFilter = parseContext.cacheFilter(parentDocMapper.typeFilter(), null);
        if (!deleteByQuery && scoreType != null) {
            query = new ChildrenQuery(parentType, childType, parentFilter, innerQuery, scoreType, shortCircuitParentDocSet, nonNestedDocsFilter);
        } else {
            query = new ChildrenConstantScoreQuery(innerQuery, parentType, childType, parentFilter, shortCircuitParentDocSet, nonNestedDocsFilter);
            if (deleteByQuery) {
                query = new XConstantScoreQuery(new DeleteByQueryWrappingFilter(query));
            }
        }
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        query.setBoost(boost);
        return query;
    }
}
