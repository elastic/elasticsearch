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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class BoolFilterParser implements FilterParser {

    public static final String NAME = "bool";

    @Inject
    public BoolFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        BooleanQuery boolFilter = new BooleanQuery();

        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;

        boolean hasAnyFilter = false;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    Filter filter = parseContext.parseInnerFilter();
                    if (filter != null) {
                        boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.FILTER));
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    Filter filter = parseContext.parseInnerFilter();
                    if (filter != null) {
                        boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.MUST_NOT));
                    }
                } else if ("should".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    Filter filter = parseContext.parseInnerFilter();
                    if (filter != null) {
                        boolFilter.setMinimumNumberShouldMatch(1);
                        boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.SHOULD));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] filter does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Filter filter = parseContext.parseInnerFilter();
                        if (filter != null) {
                            boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.MUST));
                        }
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Filter filter = parseContext.parseInnerFilter();
                        if (filter != null) {
                            boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.MUST_NOT));
                        }
                    }
                } else if ("should".equals(currentFieldName)) {
                    hasAnyFilter = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Filter filter = parseContext.parseInnerFilter();
                        if (filter != null) {
                            boolFilter.setMinimumNumberShouldMatch(1);
                            boolFilter.add(new BooleanClause(filter, BooleanClause.Occur.SHOULD));
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!hasAnyFilter) {
            throw new QueryParsingException(parseContext.index(), "[bool] filter has no inner should/must/must_not elements");
        }

        if (boolFilter.clauses().isEmpty()) {
            // no filters provided, it should be ignored upstream
            return null;
        }

        Filter filter = Queries.wrap(boolFilter);
        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}