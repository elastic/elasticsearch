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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.DeleteByQueryWrappingFilter;
import org.elasticsearch.index.search.child.ParentConstantScoreQuery;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class HasParentFilterParser implements FilterParser {

    public static final String NAME = "has_parent";

    @Inject
    public HasParentFilterParser() {
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
        String parentType = null;

        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String filterName = null;
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
                } else if ("filter".equals(currentFieldName)) {
                    // TODO handle `filter` element before `type` element...
                    String[] origTypes = QueryParseContext.setTypesWithPrevious(parentType == null ? null : new String[]{parentType});
                    try {
                        Filter innerFilter = parseContext.parseInnerFilter();
                        query = new XConstantScoreQuery(innerFilter);
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
                    throw new QueryParsingException(parseContext.index(), "the [_scope] support in [has_parent] filter has been removed, use a filter as a facet_filter in the relevant global facet");
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] filter requires 'query' field");
        }
        if (query == null) {
            return null;
        }

        if (parentType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] filter requires 'parent_type' field");
        }

        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] filter configured 'parent_type' [" + parentType + "] is not a valid type");
        }

        // wrap the query with type query
        query = new XFilteredQuery(query, parseContext.cacheFilter(parentDocMapper.typeFilter(), null));

        Set<String> parentTypes = new HashSet<String>(5);
        parentTypes.add(parentType);
        for (DocumentMapper documentMapper : parseContext.mapperService()) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                DocumentMapper parentTypeDocumentMapper = parseContext.mapperService().documentMapper(parentFieldMapper.type());
                if (parentTypeDocumentMapper == null) {
                    // Only add this, if this parentFieldMapper (also a parent)  isn't a child of another parent.
                    parentTypes.add(parentFieldMapper.type());
                }
            }
        }

        Filter parentFilter;
        if (parentTypes.size() == 1) {
            DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypes.iterator().next());
            parentFilter = parseContext.cacheFilter(documentMapper.typeFilter(), null);
        } else {
            XBooleanFilter parentsFilter = new XBooleanFilter();
            for (String parentTypeStr : parentTypes) {
                DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypeStr);
                Filter filter = parseContext.cacheFilter(documentMapper.typeFilter(), null);
                parentsFilter.add(filter, BooleanClause.Occur.SHOULD);
            }
            parentFilter = parentsFilter;
        }
        Filter childrenFilter = parseContext.cacheFilter(new NotFilter(parentFilter), null);
        Query parentConstantScoreQuery = new ParentConstantScoreQuery(query, parentType, childrenFilter);

        if (filterName != null) {
            parseContext.addNamedQuery(filterName, parentConstantScoreQuery);
        }

        boolean deleteByQuery = "delete_by_query".equals(SearchContext.current().source());
        if (deleteByQuery) {
            return new DeleteByQueryWrappingFilter(parentConstantScoreQuery);
        } else {
            return new CustomQueryWrappingFilter(parentConstantScoreQuery);
        }
    }

}