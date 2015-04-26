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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 *
 */
public class PrefixFilterParser implements FilterParser {

    public static final String NAME = "prefix";

    @Inject
    public PrefixFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;
        String fieldName = null;
        Object value = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    fieldName = currentFieldName;
                    value = parser.objectBytes();
                }
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for prefix filter");
        }

        Filter filter = null;

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            filter = smartNameFieldMappers.mapper().prefixFilter(value, parseContext);
        }
        if (filter == null) {
            filter = Queries.wrap(new PrefixQuery(new Term(fieldName, BytesRefs.toBytesRef(value))));
        }

        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}