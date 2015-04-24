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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class OrFilterParser implements FilterParser {

    public static final String NAME = "or";

    @Inject
    public OrFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        ArrayList<Filter> filters = newArrayList();
        boolean filtersFound = false;

        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                filtersFound = true;
                Filter filter = parseContext.parseInnerFilter();
                if (filter != null) {
                    filters.add(filter);
                }
            }
        } else {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("filters".equals(currentFieldName)) {
                        filtersFound = true;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            Filter filter = parseContext.parseInnerFilter();
                            if (filter != null) {
                                filters.add(filter);
                            }
                        }
                    } else {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            filtersFound = true;
                            Filter filter = parseContext.parseInnerFilter();
                            if (filter != null) {
                                filters.add(filter);
                            }
                        }
                    }
                } else if (token.isValue()) {
                    if ("_cache".equals(currentFieldName)) {
                        cache = parseContext.parseFilterCachePolicy();
                    } else if ("_name".equals(currentFieldName)) {
                        filterName = parser.text();
                    } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                        cacheKey = new HashedBytesRef(parser.text());
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[or] filter does not support [" + currentFieldName + "]");
                    }
                }
            }
        }

        if (!filtersFound) {
            throw new QueryParsingException(parseContext.index(), "[or] filter requires 'filters' to be set on it'");
        }

        if (filters.isEmpty()) {
            return null;
        }

        // no need to cache this one
        BooleanQuery boolQuery = new BooleanQuery();
        for (Filter filter : filters) {
            boolQuery.add(filter, Occur.SHOULD);
        }
        Filter filter = Queries.wrap(boolQuery);
        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}