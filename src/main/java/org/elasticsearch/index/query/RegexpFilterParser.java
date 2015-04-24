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
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Operations;
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
public class RegexpFilterParser implements FilterParser {

    public static final String NAME = "regexp";

    @Inject
    public RegexpFilterParser() {
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
        String secondaryFieldName = null;
        Object value = null;
        Object secondaryValue = null;
        int flagsValue = -1;

        String filterName = null;
        String currentFieldName = null;
        int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName)) {
                            value = parser.objectBytes();
                        } else if ("flags".equals(currentFieldName)) {
                            String flags = parser.textOrNull();
                            flagsValue = RegexpFlag.resolveValue(flags);
                        } else if ("max_determinized_states".equals(currentFieldName)) {
                            maxDeterminizedStates = parser.intValue();
                        } else if ("flags_value".equals(currentFieldName)) {
                            flagsValue = parser.intValue();
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[regexp] filter does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    secondaryFieldName = currentFieldName;
                    secondaryValue = parser.objectBytes();
                }
            }
        }

        if (fieldName == null) {
            fieldName = secondaryFieldName;
            value = secondaryValue;
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for regexp filter");
        }

        Filter filter = null;

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            filter = smartNameFieldMappers.mapper().regexpFilter(value, flagsValue, maxDeterminizedStates, parseContext);
        }
        if (filter == null) {
            filter = Queries.wrap(new RegexpQuery(new Term(fieldName, BytesRefs.toBytesRef(value)), flagsValue, maxDeterminizedStates));
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
