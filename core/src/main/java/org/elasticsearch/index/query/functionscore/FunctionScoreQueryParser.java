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
import org.elasticsearch.common.inject.Inject;
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

    private final ScoreFunctionParserMapper functionParserMapper;

    @Inject
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
            } else if ("query".equals(currentFieldName)) {
                query = parseContext.parseInnerQueryBuilder();
            } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                scoreMode = FiltersFunctionScoreQuery.ScoreMode.fromString(parser.text());
            } else if ("boost_mode".equals(currentFieldName) || "boostMode".equals(currentFieldName)) {
                combineFunction = CombineFunction.fromString(parser.text());
            } else if ("max_boost".equals(currentFieldName) || "maxBoost".equals(currentFieldName)) {
                maxBoost = parser.floatValue();
            } else if ("boost".equals(currentFieldName)) {
                boost = parser.floatValue();
            } else if ("_name".equals(currentFieldName)) {
                queryName = parser.text();
            } else if ("min_score".equals(currentFieldName) || "minScore".equals(currentFieldName)) {
                minScore = parser.floatValue();
            } else if ("functions".equals(currentFieldName)) {
                if (singleFunctionFound) {
                    String errorString = "already found [" + singleFunctionName + "], now encountering [functions].";
                    handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                }
                functionArrayFound = true;
                currentFieldName = parseFiltersAndFunctions(parseContext, parser, filterFunctionBuilders);
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

                ScoreFunctionBuilder<?> scoreFunction;
                if (parseContext.parseFieldMatcher().match(currentFieldName, WEIGHT_FIELD)) {
                    scoreFunction = new WeightBuilder().setWeight(parser.floatValue());
                } else {
                    // we try to parse a score function. If there is no score
                    // function for the current field name,
                    // functionParserMapper.get() will throw an Exception.
                    scoreFunction = functionParserMapper.get(parser.getTokenLocation(), currentFieldName).fromXContent(parseContext, parser);
                }
                filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction));
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
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, WEIGHT_FIELD)) {
                        functionWeight = parser.floatValue();
                    } else {
                        if ("filter".equals(currentFieldName)) {
                            filter = parseContext.parseInnerQueryBuilder();
                        } else {
                            if (scoreFunction != null) {
                                throw new ParsingException(parser.getTokenLocation(), "failed to parse function_score functions. already found [{}], now encountering [{}].", scoreFunction.getName(), currentFieldName);
                            }
                            // do not need to check null here,
                            // functionParserMapper throws exception if parser
                            // non-existent
                            ScoreFunctionParser functionParser = functionParserMapper.get(parser.getTokenLocation(), currentFieldName);
                            scoreFunction = functionParser.fromXContent(parseContext, parser);
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
