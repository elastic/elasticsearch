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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for boosting query
 */
public class BoostingQueryParser implements QueryParser<BoostingQueryBuilder> {

    public static final ParseField POSITIVE_FIELD = new ParseField("positive");
    public static final ParseField NEGATIVE_FIELD = new ParseField("negative");
    public static final ParseField NEGATIVE_BOOST_FIELD = new ParseField("negative_boost");

    @Override
    public String[] names() {
        return new String[]{BoostingQueryBuilder.NAME};
    }

    @Override
    public BoostingQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        QueryBuilder positiveQuery = null;
        boolean positiveQueryFound = false;
        QueryBuilder negativeQuery = null;
        boolean negativeQueryFound = false;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        float negativeBoost = -1;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, POSITIVE_FIELD)) {
                    positiveQuery = parseContext.parseInnerQueryBuilder();
                    positiveQueryFound = true;
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, NEGATIVE_FIELD)) {
                    negativeQuery = parseContext.parseInnerQueryBuilder();
                    negativeQueryFound = true;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NEGATIVE_BOOST_FIELD)) {
                    negativeBoost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!positiveQueryFound) {
            throw new ParsingException(parser.getTokenLocation(), "[boosting] query requires 'positive' query to be set'");
        }
        if (!negativeQueryFound) {
            throw new ParsingException(parser.getTokenLocation(), "[boosting] query requires 'negative' query to be set'");
        }
        if (negativeBoost < 0) {
            throw new ParsingException(parser.getTokenLocation(), "[boosting] query requires 'negative_boost' to be set to be a positive value'");
        }

        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positiveQuery, negativeQuery);
        boostingQuery.negativeBoost(negativeBoost);
        boostingQuery.boost(boost);
        boostingQuery.queryName(queryName);
        return boostingQuery;
    }

    @Override
    public BoostingQueryBuilder getBuilderPrototype() {
        return BoostingQueryBuilder.PROTOTYPE;
    }
}