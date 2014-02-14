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

import org.apache.lucene.queries.BoostingQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class BoostingQueryParser implements QueryParser {

    public static final String NAME = "boosting";

    @Inject
    public BoostingQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query positiveQuery = null;
        boolean positiveQueryFound = false;
        Query negativeQuery = null;
        boolean negativeQueryFound = false;
        float boost = -1;
        float negativeBoost = -1;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("positive".equals(currentFieldName)) {
                    positiveQuery = parseContext.parseInnerQuery();
                    positiveQueryFound = true;
                } else if ("negative".equals(currentFieldName)) {
                    negativeQuery = parseContext.parseInnerQuery();
                    negativeQueryFound = true;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("negative_boost".equals(currentFieldName) || "negativeBoost".equals(currentFieldName)) {
                    negativeBoost = parser.floatValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (positiveQuery == null && !positiveQueryFound) {
            throw new QueryParsingException(parseContext.index(), "[boosting] query requires 'positive' query to be set'");
        }
        if (negativeQuery == null && !negativeQueryFound) {
            throw new QueryParsingException(parseContext.index(), "[boosting] query requires 'negative' query to be set'");
        }
        if (negativeBoost == -1) {
            throw new QueryParsingException(parseContext.index(), "[boosting] query requires 'negative_boost' to be set'");
        }

        // parsers returned null
        if (positiveQuery == null || negativeQuery == null) {
            return null;
        }

        BoostingQuery boostingQuery = new BoostingQuery(positiveQuery, negativeQuery, negativeBoost);
        if (boost != -1) {
            boostingQuery.setBoost(boost);
        }
        return boostingQuery;
    }
}