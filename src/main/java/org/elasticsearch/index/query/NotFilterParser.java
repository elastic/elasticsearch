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
import org.apache.lucene.search.QueryWrapperFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class NotFilterParser implements FilterParser {

    public static final String NAME = "not";

    @Inject
    public NotFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Filter filter = null;
        boolean filterFound = false;
        boolean cache = false;
        HashedBytesRef cacheKey = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                    filterFound = true;
                } else {
                    filterFound = true;
                    // its the filter, and the name is the field
                    filter = parseContext.parseInnerFilter(currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                filterFound = true;
                // its the filter, and the name is the field
                filter = parseContext.parseInnerFilter(currentFieldName);
            } else if (token.isValue()) {
                if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[not] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!filterFound) {
            throw new QueryParsingException(parseContext.index(), "filter is required when using `not` filter");
        }

        if (filter == null) {
            return null;
        }

        Filter notFilter = Queries.wrap(Queries.not(filter));
        if (cache) {
            notFilter = parseContext.cacheFilter(notFilter, cacheKey, parseContext.autoFilterCachePolicy());
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, notFilter);
        }
        return notFilter;
    }
}