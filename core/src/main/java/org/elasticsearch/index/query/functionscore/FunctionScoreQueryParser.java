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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.factor.FactorParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Parser for function_score query
 */
public class FunctionScoreQueryParser extends BaseQueryParser {

    // For better readability of error message
    static final String MISPLACED_FUNCTION_MESSAGE_PREFIX = "you can either define [functions] array or a single function, not both. ";
    static final String MISPLACED_BOOST_FUNCTION_MESSAGE_SUFFIX = " did you mean [boost] instead?";

    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    private static final ParseField FILTER_FIELD = new ParseField("filter").withAllDeprecated("query");

    ScoreFunctionParserMapper functionParserMapper;

    @Inject
    public FunctionScoreQueryParser(ScoreFunctionParserMapper functionParserMapper) {
        this.functionParserMapper = functionParserMapper;
    }

    @Override
    public String[] names() {
        return new String[] { FunctionScoreQueryBuilder.NAME, Strings.toCamelCase(FunctionScoreQueryBuilder.NAME) };
    }

    static final ImmutableMap<String, CombineFunction> combineFunctionsMap;

    static {
        CombineFunction[] values = CombineFunction.values();
        Builder<String, CombineFunction> combineFunctionMapBuilder = ImmutableMap.builder();
        for (CombineFunction combineFunction : values) {
            combineFunctionMapBuilder.put(combineFunction.getName(), combineFunction);
        }
        combineFunctionsMap = combineFunctionMapBuilder.build();
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext context) throws IOException, QueryParsingException {
        XContentParser parser = context.parser();

        QueryBuilder queryBuilder = null;
        QueryBuilder filterBuilder = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = FunctionScoreQueryBuilder.DEFAULT_SCORE_MODE;
        ArrayList<Tuple<QueryBuilder, ScoreFunctionBuilder>> filterFunctions = new ArrayList<>();
        Float maxBoost = null;
        Float minScore = null;

        String currentFieldName = null;
        XContentParser.Token token;
        CombineFunction combineFunction = FunctionScoreQueryBuilder.DEFAULT_BOOST_MODE;
        // Either define array of functions and filters or only one function
        boolean functionArrayFound = false;
        boolean singleFunctionFound = false;
        String singleFunctionName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("query".equals(currentFieldName)) {
                queryBuilder = context.parseInnerQueryBuilder();
            } else if (context.parseFieldMatcher().match(currentFieldName, FILTER_FIELD)) {
                filterBuilder = context.parseInnerFilterToQueryBuilder();
            } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                scoreMode = parseScoreMode(context, parser);
            } else if ("boost_mode".equals(currentFieldName) || "boostMode".equals(currentFieldName)) {
                combineFunction = parseBoostMode(context, parser);
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
                    handleMisplacedFunctionsDeclaration(errorString, singleFunctionName);
                }
                currentFieldName = parseFiltersAndFunctions(context, parser, filterFunctions, currentFieldName);
                functionArrayFound = true;
            } else {
                ScoreFunctionBuilder scoreFunction;
                if (currentFieldName.equals("weight")) {
                    scoreFunction = ScoreFunctionBuilders.weightFactorFunction(parser.floatValue());
                } else {
                    // we try to parse a score function. If there is no score
                    // function for the current field name,
                    // functionParserMapper.get() will throw an Exception.
                    scoreFunction = functionParserMapper.get(context, currentFieldName).fromXContent(context, parser);
                }
                if (functionArrayFound) {
                    String errorString = "already found [functions] array, now encountering [" + currentFieldName + "].";
                    handleMisplacedFunctionsDeclaration(errorString, currentFieldName);
                }
                if (filterFunctions.size() > 0) {
                    throw new ElasticsearchParseException("failed to parse [{}] query. already found function [{}], " +
                            "now encountering [{}]. use [functions] array if you want to define several functions.",
                            FunctionScoreQueryBuilder.NAME, singleFunctionName, currentFieldName);
                }
                filterFunctions.add(new Tuple(null, scoreFunction));
                singleFunctionFound = true;
                singleFunctionName = currentFieldName;
            }
        }
        return new FunctionScoreQueryBuilder(queryBuilder, filterBuilder)
                .addAll(filterFunctions)
                .scoreMode(scoreMode)
                .boostMode(combineFunction)
                .minScore(minScore)
                .maxBoost(maxBoost)
                .boost(boost)
                .queryName(queryName);
    }

    private void handleMisplacedFunctionsDeclaration(String errorString, String functionName) {
        errorString = MISPLACED_FUNCTION_MESSAGE_PREFIX + errorString;
        if (Arrays.asList(FactorParser.NAMES).contains(functionName)) {
            errorString = errorString + MISPLACED_BOOST_FUNCTION_MESSAGE_SUFFIX;
        }
        throw new ElasticsearchParseException("failed to parse [{}] query. [{}]", FunctionScoreQueryBuilder.NAME, errorString);
    }

    private String parseFiltersAndFunctions(QueryParseContext parseContext, XContentParser parser,
                                            ArrayList<Tuple<QueryBuilder, ScoreFunctionBuilder>> filterFunctions, String currentFieldName) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            QueryBuilder filter = null;
            ScoreFunctionBuilder scoreFunction = null;
            Float functionWeight = null;
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(parseContext, "failed to parse [{}]. malformed query, expected a [{}] " +
                        "while parsing functions but got a [{}] instead", XContentParser.Token.START_OBJECT, token,
                        FunctionScoreQueryBuilder.NAME);
            } else {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, WEIGHT_FIELD)) {
                        functionWeight = parser.floatValue();
                    } else {
                        if ("filter".equals(currentFieldName)) {
                            filter = parseContext.parseInnerFilterToQueryBuilder();
                        } else {
                            // do not need to check null here,
                            // functionParserMapper throws exception if parser
                            // non-existent
                            ScoreFunctionParser functionParser = functionParserMapper.get(parseContext, currentFieldName);
                            scoreFunction = functionParser.fromXContent(parseContext, parser);
                        }
                    }
                }
                if (functionWeight != null) {
                    // handle the case where we just have a weight but no function
                    if (scoreFunction == null) {
                        scoreFunction = ScoreFunctionBuilders.weightFactorFunction(functionWeight);
                    } else {
                        scoreFunction = scoreFunction.setWeight(functionWeight);
                    }
                }
            }
            if (scoreFunction == null) {
                throw new ElasticsearchParseException("failed to parse [{}] query. an entry in functions list is missing a function.", FunctionScoreQueryBuilder.NAME);
            }
            filterFunctions.add(new Tuple(filter, scoreFunction));
        }
        return currentFieldName;
    }

    private FiltersFunctionScoreQuery.ScoreMode parseScoreMode(QueryParseContext parseContext, XContentParser parser) throws IOException {
        String scoreMode = parser.text();
        if ("avg".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Avg;
        } else if ("max".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Max;
        } else if ("min".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Min;
        } else if ("sum".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Sum;
        } else if ("multiply".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Multiply;
        } else if ("first".equals(scoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.First;
        } else {
            throw new QueryParsingException(parseContext, "failed to parse [{}] query. illegal score_mode [{}]", FunctionScoreQueryBuilder.NAME, scoreMode);
        }
    }

    private CombineFunction parseBoostMode(QueryParseContext parseContext, XContentParser parser) throws IOException {
        String boostMode = parser.text();
        CombineFunction cf = combineFunctionsMap.get(boostMode);
        if (cf == null) {
            throw new QueryParsingException(parseContext, "failed to parse [{}] query. illegal boost_mode [{}]", FunctionScoreQueryBuilder.NAME, boostMode);
        }
        return cf;
    }

    //norelease to be removed once all queries are moved over to extend BaseQueryParser
    @Override
    public FunctionScoreQueryBuilder getBuilderPrototype() {
        return FunctionScoreQueryBuilder.PROTOTYPE;
    }
}
