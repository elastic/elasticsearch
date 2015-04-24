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
package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.ChildrenQuery;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryParserUtils.ensureNotDeleteByQuery;

/**
 *
 */
public class HasChildFilterParser implements FilterParser {

    public static final String NAME = "has_child";

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasChildFilterParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        ensureNotDeleteByQuery(NAME, parseContext);
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        boolean filterFound = false;
        String childType = null;
        int shortCircuitParentDocSet = 8192; // Tests show a cut of point between 8192 and 16384.
        int minChildren = 0;
        int maxChildren = 0;
        Tuple<String, SubSearchContext> innerHits = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        XContentStructure.InnerQuery innerQuery = null;
        XContentStructure.InnerFilter innerFilter = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Usually, the query would be parsed here, but the child
                // type may not have been extracted yet, so use the
                // XContentStructure.<type> facade to parse if available,
                // or delay parsing if not.
                if ("query".equals(currentFieldName)) {
                    innerQuery = new XContentStructure.InnerQuery(parseContext, childType == null ? null : new String[] {childType});
                    queryFound = true;
                } else if ("filter".equals(currentFieldName)) {
                    innerFilter = new XContentStructure.InnerFilter(parseContext, childType == null ? null : new String[] {childType});
                    filterFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else if ("short_circuit_cutoff".equals(currentFieldName)) {
                    shortCircuitParentDocSet = parser.intValue();
                } else if ("min_children".equals(currentFieldName) || "minChildren".equals(currentFieldName)) {
                    minChildren = parser.intValue(true);
                } else if ("max_children".equals(currentFieldName) || "maxChildren".equals(currentFieldName)) {
                    maxChildren = parser.intValue(true);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound && !filterFound) {
            throw new QueryParsingException(parseContext.index(), "[has_child] filter requires 'query' or 'filter' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] filter requires 'type' field");
        }

        Query query;
        if (queryFound) {
            query = innerQuery.asQuery(childType);
        } else {
            query = innerFilter.asFilter(childType);
        }

        if (query == null) {
            return null;
        }

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "No mapping for for type [" + childType + "]");
        }
        if (innerHits != null) {
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.v2(), query, null, childDocMapper);
            String name = innerHits.v1() != null ? innerHits.v1() : childType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (!parentFieldMapper.active()) {
            throw new QueryParsingException(parseContext.index(), "Type [" + childType + "] does not have parent mapping");
        }
        String parentType = parentFieldMapper.type();

        // wrap the query with type query
        query = new FilteredQuery(query, parseContext.cacheFilter(childDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy()));

        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] points to a non existent parent type [" + parentType + "]");
        }

        if (maxChildren > 0 && maxChildren < minChildren) {
            throw new QueryParsingException(parseContext.index(), "[has_child] 'max_children' is less than 'min_children'");
        }

        BitDocIdSetFilter nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
        }

        Filter parentFilter = parseContext.cacheFilter(parentDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy());
        ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper);

        Query childrenQuery;
        if (minChildren > 1 || maxChildren > 0) {
            childrenQuery = new ChildrenQuery(parentChildIndexFieldData,  parentType, childType, parentFilter,query,ScoreType.NONE,minChildren, maxChildren, shortCircuitParentDocSet, nonNestedDocsFilter);
        } else {
            childrenQuery = new ChildrenConstantScoreQuery(parentChildIndexFieldData, query, parentType, childType, parentFilter,
                    shortCircuitParentDocSet, nonNestedDocsFilter);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, new CustomQueryWrappingFilter(childrenQuery));
        }
        return new CustomQueryWrappingFilter(childrenQuery);
    }

}
