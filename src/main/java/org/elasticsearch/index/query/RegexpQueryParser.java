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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

/**
 *
 */
public class RegexpQueryParser implements QueryParser {

    public static final String NAME = "regexp";

    @Inject
    public RegexpQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[regexp] query malformed, no field");
        }
        String fieldName = parser.currentName();
        String rewriteMethod = null;

        Object value = null;
        float boost = 1.0f;
        int flagsValue = -1;
        int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
        String queryName = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewriteMethod = parser.textOrNull();
                    } else if ("flags".equals(currentFieldName)) {
                        String flags = parser.textOrNull();
                        flagsValue = RegexpFlag.resolveValue(flags);
                    } else if ("flags_value".equals(currentFieldName)) {
                        flagsValue = parser.intValue();
                        if (flagsValue < 0) {
                            flagsValue = RegExp.ALL;
                        }
                    } else if ("maxDeterminizedStates".equals(currentFieldName)) {
                        maxDeterminizedStates = parser.intValue();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[regexp] query does not support [" + currentFieldName + "]");
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectBytes();
            parser.nextToken();
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for regexp query");
        }

        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(rewriteMethod, null);

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().regexpQuery(value, flagsValue, maxDeterminizedStates, method, parseContext);
        }
        if (query == null) {
            RegexpQuery regexpQuery = new RegexpQuery(new Term(fieldName, BytesRefs.toBytesRef(value)), flagsValue, maxDeterminizedStates);
            if (method != null) {
                regexpQuery.setRewriteMethod(method);
            }
            query = regexpQuery;
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }


}
