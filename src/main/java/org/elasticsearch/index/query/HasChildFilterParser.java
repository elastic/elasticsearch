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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.DeleteByQueryWrappingFilter;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class HasChildFilterParser implements FilterParser {

    public static final String NAME = "has_child";

    @Inject
    public HasChildFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        String childType = null;
        int shortCircuitParentDocSet = 8192; // Tests show a cut of point between 8192 and 16384.

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    // TODO we need to set the type, but, `query` can come before `type`...
                    // since we switch types, make sure we change the context
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(childType == null ? null : new String[]{childType});
                    try {
                        query = parseContext.parseInnerQuery();
                        queryFound = true;
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else if ("filter".equals(currentFieldName)) {
                    // TODO handle `filter` element before `type` element...
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(childType == null ? null : new String[]{childType});
                    try {
                        Filter innerFilter = parseContext.parseInnerFilter();
                        query = new XConstantScoreQuery(innerFilter);
                        queryFound = true;
                    } finally {
                        QueryParseContext.setTypes(origTypes);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("_scope".equals(currentFieldName)) {
                    throw new QueryParsingException(parseContext.index(), "the [_scope] support in [has_child] filter has been removed, use a filter as a facet_filter in the relevant global facet");
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else if ("short_circuit_cutoff".equals(currentFieldName)) {
                    shortCircuitParentDocSet = parser.intValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[has_child] filter requires 'query' field");
        }
        if (query == null) {
            return null;
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] filter requires 'type' field");
        }

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "No mapping for for type [" + childType + "]");
        }
        if (!childDocMapper.parentFieldMapper().active()) {
            throw new QueryParsingException(parseContext.index(), "Type [" + childType + "] does not have parent mapping");
        }
        String parentType = childDocMapper.parentFieldMapper().type();

        // wrap the query with type query
        query = new XFilteredQuery(query, parseContext.cacheFilter(childDocMapper.typeFilter(), null));

        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] points to a non existent parent type [" + parentType + "]");
        }

        Filter nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.cacheFilter(NonNestedDocsFilter.INSTANCE, null);
        }

        Filter parentFilter = parseContext.cacheFilter(parentDocMapper.typeFilter(), null);
        Query childrenConstantScoreQuery = new ChildrenConstantScoreQuery(query, parentType, childType, parentFilter, shortCircuitParentDocSet, nonNestedDocsFilter);

        if (filterName != null) {
            parseContext.addNamedQuery(filterName, childrenConstantScoreQuery);
        }

        boolean deleteByQuery = "delete_by_query".equals(SearchContext.current().source());
        if (deleteByQuery) {
            return new DeleteByQueryWrappingFilter(childrenConstantScoreQuery);
        } else {
            return new CustomQueryWrappingFilter(childrenConstantScoreQuery);
        }
    }
}
