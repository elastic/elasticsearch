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
public class HasChildQueryParser implements QueryParser {

    public static final String NAME = "has_child";

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasChildQueryParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        ensureNotDeleteByQuery(NAME, parseContext);
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreType scoreType = ScoreType.NONE;
        int minChildren = 0;
        int maxChildren = 0;
        int shortCircuitParentDocSet = 8192;
        String queryName = null;
        Tuple<String, SubSearchContext> innerHits = null;

        String currentFieldName = null;
        XContentParser.Token token;
        XContentStructure.InnerQuery iq = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Usually, the query would be parsed here, but the child
                // type may not have been extracted yet, so use the
                // XContentStructure.<type> facade to parse if available,
                // or delay parsing if not.
                if ("query".equals(currentFieldName)) {
                    iq = new XContentStructure.InnerQuery(parseContext, childType == null ? null : new String[] { childType });
                    queryFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("score_type".equals(currentFieldName) || "scoreType".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("min_children".equals(currentFieldName) || "minChildren".equals(currentFieldName)) {
                    minChildren = parser.intValue(true);
                } else if ("max_children".equals(currentFieldName) || "maxChildren".equals(currentFieldName)) {
                    maxChildren = parser.intValue(true);
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
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'type' field");
        }

        Query innerQuery = iq.asQuery(childType);

        if (innerQuery == null) {
            return null;
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] No mapping for for type [" + childType + "]");
        }
        if (!childDocMapper.parentFieldMapper().active()) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] does not have parent mapping");
        }

        if (innerHits != null) {
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.v2(), innerQuery, null, childDocMapper);
            String name = innerHits.v1() != null ? innerHits.v1() : childType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }

        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (!parentFieldMapper.active()) {
            throw new QueryParsingException(parseContext.index(), "[has_child] _parent field not configured");
        }

        String parentType = parentFieldMapper.type();
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType
                    + "] points to a non existent parent type [" + parentType + "]");
        }

        if (maxChildren > 0 && maxChildren < minChildren) {
            throw new QueryParsingException(parseContext.index(), "[has_child] 'max_children' is less than 'min_children'");
        }

        BitDocIdSetFilter nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
        }

        // wrap the query with type query
        innerQuery = new FilteredQuery(innerQuery, parseContext.cacheFilter(childDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy()));

        Query query;
        Filter parentFilter = parseContext.cacheFilter(parentDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy());
        ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper);
        if (minChildren > 1 || maxChildren > 0 || scoreType != ScoreType.NONE) {
            query = new ChildrenQuery(parentChildIndexFieldData, parentType, childType, parentFilter, innerQuery, scoreType, minChildren,
                    maxChildren, shortCircuitParentDocSet, nonNestedDocsFilter);
        } else {
            query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, innerQuery, parentType, childType, parentFilter,
                    shortCircuitParentDocSet, nonNestedDocsFilter);
        }
        if (queryName != null) {
            parseContext.addNamedFilter(queryName, new CustomQueryWrappingFilter(query));
        }
        query.setBoost(boost);
        return query;
    }
}
