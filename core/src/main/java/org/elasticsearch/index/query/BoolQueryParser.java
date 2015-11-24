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

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for bool query
 */
public class BoolQueryParser implements QueryParser<BoolQueryBuilder> {

    public static final String MUSTNOT = "mustNot";
    public static final String MUST_NOT = "must_not";
    public static final String FILTER = "filter";
    public static final String SHOULD = "should";
    public static final String MUST = "must";
    public static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord");
    public static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
    public static final ParseField MINIMUM_NUMBER_SHOULD_MATCH = new ParseField("minimum_number_should_match");
    public static final ParseField ADJUST_PURE_NEGATIVE = new ParseField("adjust_pure_negative");

    @Inject
    public BoolQueryParser(Settings settings) {
        BooleanQuery.setMaxClauseCount(settings.getAsInt("index.query.bool.max_clause_count", settings.getAsInt("indices.query.bool.max_clause_count", BooleanQuery.getMaxClauseCount())));
    }

    @Override
    public String[] names() {
        return new String[]{BoolQueryBuilder.NAME};
    }

    @Override
    public BoolQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();

        boolean disableCoord = BoolQueryBuilder.DISABLE_COORD_DEFAULT;
        boolean adjustPureNegative = BoolQueryBuilder.ADJUST_PURE_NEGATIVE_DEFAULT;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String minimumShouldMatch = null;

        final List<QueryBuilder> mustClauses = new ArrayList<>();
        final List<QueryBuilder> mustNotClauses = new ArrayList<>();
        final List<QueryBuilder> shouldClauses = new ArrayList<>();
        final List<QueryBuilder> filterClauses = new ArrayList<>();
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        QueryBuilder query;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                case MUST:
                    query = parseContext.parseInnerQueryBuilder();
                    mustClauses.add(query);
                    break;
                case SHOULD:
                    query = parseContext.parseInnerQueryBuilder();
                    shouldClauses.add(query);
                    break;
                case FILTER:
                    query = parseContext.parseInnerQueryBuilder();
                    filterClauses.add(query);
                    break;
                case MUST_NOT:
                case MUSTNOT:
                    query = parseContext.parseInnerQueryBuilder();
                    mustNotClauses.add(query);
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "[bool] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    switch (currentFieldName) {
                    case MUST:
                        query = parseContext.parseInnerQueryBuilder();
                        mustClauses.add(query);
                        break;
                    case SHOULD:
                        query = parseContext.parseInnerQueryBuilder();
                        shouldClauses.add(query);
                        break;
                    case FILTER:
                        query = parseContext.parseInnerQueryBuilder();
                        filterClauses.add(query);
                        break;
                    case MUST_NOT:
                    case MUSTNOT:
                        query = parseContext.parseInnerQueryBuilder();
                        mustNotClauses.add(query);
                        break;
                    default:
                        throw new ParsingException(parser.getTokenLocation(), "bool query does not support [" + currentFieldName + "]");
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, DISABLE_COORD_FIELD)) {
                    disableCoord = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_NUMBER_SHOULD_MATCH)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ADJUST_PURE_NEGATIVE)) {
                    adjustPureNegative = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[bool] query does not support [" + currentFieldName + "]");
                }
            }
        }
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (QueryBuilder queryBuilder : mustClauses) {
            boolQuery.must(queryBuilder);
        }
        for (QueryBuilder queryBuilder : mustNotClauses) {
            boolQuery.mustNot(queryBuilder);
        }
        for (QueryBuilder queryBuilder : shouldClauses) {
            boolQuery.should(queryBuilder);
        }
        for (QueryBuilder queryBuilder : filterClauses) {
            boolQuery.filter(queryBuilder);
        }
        boolQuery.boost(boost);
        boolQuery.disableCoord(disableCoord);
        boolQuery.adjustPureNegative(adjustPureNegative);
        boolQuery.minimumNumberShouldMatch(minimumShouldMatch);
        boolQuery.queryName(queryName);
        return boolQuery;
    }

    @Override
    public BoolQueryBuilder getBuilderPrototype() {
        return BoolQueryBuilder.PROTOTYPE;
    }
}
