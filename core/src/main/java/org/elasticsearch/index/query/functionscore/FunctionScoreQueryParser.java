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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.EmptyQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for function_score query
 */
public class FunctionScoreQueryParser implements QueryParser<FunctionScoreQueryBuilder> {

    private static final FunctionScoreQueryBuilder PROTOTYPE = new FunctionScoreQueryBuilder(EmptyQueryBuilder.PROTOTYPE, new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]);

    // For better readability of error message
    static final String MISPLACED_FUNCTION_MESSAGE_PREFIX = "you can either define [functions] array or a single function, not both. ";

    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField FUNCTIONS_FIELD = new ParseField("functions");
    public static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    public static final ParseField BOOST_MODE_FIELD = new ParseField("boost_mode");
    public static final ParseField MAX_BOOST_FIELD = new ParseField("max_boost");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    private final ScoreFunctionParserMapper functionParserMapper;

    public FunctionScoreQueryParser(ScoreFunctionParserMapper functionParserMapper) {
        this.functionParserMapper = functionParserMapper;
    }

    @Override
    public String[] names() {
        return new String[] { FunctionScoreQueryBuilder.NAME, Strings.toCamelCase(FunctionScoreQueryBuilder.NAME) };
    }

    @Override
    public FunctionScoreQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        QueryBuilder query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = FunctionScoreQueryBuilder.DEFAULT_SCORE_MODE;
        float maxBoost = FunctionScoreQuery.DEFAULT_MAX_BOOST;
        Float minScore = null;

        String currentFieldName = null;
        XContentParser.Token token;
        CombineFunction combineFunction = null;
        // Either define array of functions and filters or only one function
        boolean functionArrayFound = false;
        boolean singleFunctionFound = false;
        String singleFunctionName = null;
        List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    if (query != null) {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. [query] is already defined.", FunctionScoreQueryBuilder.NAME);
                    }
                    query = parseContext.parseInnerQueryBuilder();
                } else {
                    if (singleFunctionFound) {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. already found function [{}], now encountering [{}]. use [functions] array if you want to define several functions.", FunctionScoreQueryBuilder.NAME, singleFunctionName, currentFieldName);
                    }
                    if (functionArrayFound) {
                        String errorString = "already found [functions] array, now encountering [" + currentFieldName + "].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    singleFunctionFound = true;
                    singleFunctionName = currentFieldName;

                    // we try to parse a score function. If there is no score function for the current field name,
                    // functionParserMapper.get() may throw an Exception.
                    ScoreFunctionBuilder<?> scoreFunction = functionParserMapper.get(parser.getTokenLocation(), currentFieldName).fromXContent(parseContext, parser);
                    filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, FUNCTIONS_FIELD)) {
                    if (singleFunctionFound) {
                        String errorString = "already found [" + singleFunctionName + "], now encountering [functions].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    functionArrayFound = true;
                    currentFieldName = parseFiltersAndFunctions(parseContext, parser, filterFunctionBuilders);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. array [{}] is not supported", FunctionScoreQueryBuilder.NAME, currentFieldName);
                }

            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, SCORE_MODE_FIELD)) {
                    scoreMode = FiltersFunctionScoreQuery.ScoreMode.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, BOOST_MODE_FIELD)) {
                    combineFunction = CombineFunction.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MAX_BOOST_FIELD)) {
                    maxBoost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MIN_SCORE_FIELD)) {
                    minScore = parser.floatValue();
                } else {
                    if (singleFunctionFound) {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. already found function [{}], now encountering [{}]. use [functions] array if you want to define several functions.", FunctionScoreQueryBuilder.NAME, singleFunctionName, currentFieldName);
                    }
                    if (functionArrayFound) {
                        String errorString = "already found [functions] array, now encountering [" + currentFieldName + "].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    if (parseContext.parseFieldMatcher().match(currentFieldName, WEIGHT_FIELD)) {
                        filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(new WeightBuilder().setWeight(parser.floatValue())));
                        singleFunctionFound = true;
                        singleFunctionName = currentFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported", FunctionScoreQueryBuilder.NAME, currentFieldName);
                    }
                }
            }
        }

        if (query == null) {
            query = new MatchAllQueryBuilder();
        }

        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(query,
                filterFunctionBuilders.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[filterFunctionBuilders.size()]));
        if (combineFunction != null) {
            functionScoreQueryBuilder.boostMode(combineFunction);
        }
        functionScoreQueryBuilder.scoreMode(scoreMode);
        functionScoreQueryBuilder.maxBoost(maxBoost);
        if (minScore != null) {
            functionScoreQueryBuilder.setMinScore(minScore);
        }
        functionScoreQueryBuilder.boost(boost);
        functionScoreQueryBuilder.queryName(queryName);
        return functionScoreQueryBuilder;
    }

    private static void handleMisplacedFunctionsDeclaration(XContentLocation contentLocation, String errorString) {
        throw new ParsingException(contentLocation, "failed to parse [{}] query. [{}]", FunctionScoreQueryBuilder.NAME, MISPLACED_FUNCTION_MESSAGE_PREFIX + errorString);
    }

    private String parseFiltersAndFunctions(QueryParseContext parseContext, XContentParser parser, List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            QueryBuilder filter = null;
            ScoreFunctionBuilder<?> scoreFunction = null;
            Float functionWeight = null;
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}]. malformed query, expected a [{}] while parsing functions but got a [{}] instead", XContentParser.Token.START_OBJECT, token, FunctionScoreQueryBuilder.NAME);
            } else {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (parseContext.parseFieldMatcher().match(currentFieldName, FILTER_FIELD)) {
                            filter = parseContext.parseInnerQueryBuilder();
                        } else {
                            if (scoreFunction != null) {
                                throw new ParsingException(parser.getTokenLocation(), "failed to parse function_score functions. already found [{}], now encountering [{}].", scoreFunction.getName(), currentFieldName);
                            }
                            // do not need to check null here, functionParserMapper does it already
                            ScoreFunctionParser functionParser = functionParserMapper.get(parser.getTokenLocation(), currentFieldName);
                            scoreFunction = functionParser.fromXContent(parseContext, parser);
                        }
                    } else if (token.isValue()) {
                        if (parseContext.parseFieldMatcher().match(currentFieldName, WEIGHT_FIELD)) {
                            functionWeight = parser.floatValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported", FunctionScoreQueryBuilder.NAME, currentFieldName);
                        }
                    }
                }
                if (functionWeight != null) {
                    if (scoreFunction == null) {
                        scoreFunction = new WeightBuilder().setWeight(functionWeight);
                    } else {
                        scoreFunction.setWeight(functionWeight);
                    }
                }
            }
            if (filter == null) {
                filter = new MatchAllQueryBuilder();
            }
            if (scoreFunction == null) {
                throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. an entry in functions list is missing a function.", FunctionScoreQueryBuilder.NAME);
            }
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(filter, scoreFunction));
        }
        return currentFieldName;
    }

    @Override
    public FunctionScoreQueryBuilder getBuilderPrototype() {
        return PROTOTYPE;
    }
}
