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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class ConstantScoreQueryParser implements QueryParser {

    public static final String NAME = "constant_score";

    @Inject
    public ConstantScoreQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Filter filter = null;
        boolean filterFound = false;
        Query query = null;
        boolean queryFound = false;
        float boost = 1.0f;
        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                    filterFound = true;
                } else if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                    queryFound = true;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[constant_score] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[constant_score] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!filterFound && !queryFound) {
            throw new QueryParsingException(parseContext.index(), "[constant_score] requires either 'filter' or 'query' element");
        }

        if (query == null && filter == null) {
            return null;
        }

        if (filter != null) {
            // cache the filter if possible needed
            if (cache != null) {
                filter = parseContext.cacheFilter(filter, cacheKey, cache);
            }

            Query query1 = new ConstantScoreQuery(filter);
            query1.setBoost(boost);
            return query1;
        }
        // Query
        query = new ConstantScoreQuery(query);
        query.setBoost(boost);
        return query;
    }
}