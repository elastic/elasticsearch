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
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;

import static org.elasticsearch.index.query.HasParentQueryParser.createParentQuery;
import static org.elasticsearch.index.query.QueryParserUtils.ensureNotDeleteByQuery;

/**
 *
 */
public class HasParentFilterParser implements FilterParser {

    public static final String NAME = "has_parent";

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasParentFilterParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
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
        String parentType = null;
        Tuple<String, SubSearchContext> innerHits = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        XContentStructure.InnerQuery iq = null;
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
                    iq = new XContentStructure.InnerQuery(parseContext, parentType == null ? null : new String[] {parentType});
                    queryFound = true;
                } else if ("filter".equals(currentFieldName)) {
                    innerFilter = new XContentStructure.InnerFilter(parseContext, parentType == null ? null : new String[] {parentType});
                    filterFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "parent_type".equals(currentFieldName) || "parentType".equals(currentFieldName)) {
                    parentType = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    // noop to be backwards compatible
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound && !filterFound) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] filter requires 'query' or 'filter' field");
        }
        if (parentType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] filter requires 'parent_type' field");
        }

        Query innerQuery;
        if (queryFound) {
            innerQuery = iq.asQuery(parentType);
        } else {
            innerQuery = innerFilter.asFilter(parentType);
        }

        if (innerQuery == null) {
            return null;
        }

        Query parentQuery = createParentQuery(innerQuery, parentType, false, parseContext, innerHits);
        if (parentQuery == null) {
            return null;
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, new CustomQueryWrappingFilter(parentQuery));
        }
        return new CustomQueryWrappingFilter(parentQuery);
    }

}