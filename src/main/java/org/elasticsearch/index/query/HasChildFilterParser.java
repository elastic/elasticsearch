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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ScoreType;
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

        Query childrenQuery = HasChildQueryParser.createChildrenQuery(parseContext, childType, ScoreType.NONE, minChildren, maxChildren, shortCircuitParentDocSet, innerHits, query);
        if (childrenQuery == null) {
            return null;
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, new CustomQueryWrappingFilter(childrenQuery));
        }
        return new CustomQueryWrappingFilter(childrenQuery);
    }

}
