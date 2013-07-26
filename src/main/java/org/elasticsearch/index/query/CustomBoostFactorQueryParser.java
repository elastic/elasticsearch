/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryParser;

import java.io.IOException;

/**
 * @deprecated use {@link FunctionScoreQueryParser} instead.
 */
public class CustomBoostFactorQueryParser implements QueryParser {

    public static final String NAME = "custom_boost_factor";

    @Inject
    public CustomBoostFactorQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        float boost = 1.0f;
        float boostFactor = 1.0f;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                    queryFound = true;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_boost_factor] query does not support ["
                            + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("boost_factor".equals(currentFieldName) || "boostFactor".equals(currentFieldName)) {
                    boostFactor = parser.floatValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_boost_factor] query does not support ["
                            + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[constant_factor_query] requires 'query' element");
        }
        if (query == null) {
            return null;
        }
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(query, new BoostScoreFunction(boostFactor));
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }
}
