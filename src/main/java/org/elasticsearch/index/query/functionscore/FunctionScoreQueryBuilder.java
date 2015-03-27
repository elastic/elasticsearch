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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.factor.FactorParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 */
public class FunctionScoreQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<FunctionScoreQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private final FilterBuilder filterBuilder;

    private Float boost;

    private Float maxBoost;

    private String scoreMode;

    private String boostMode;

    private ArrayList<FilterBuilder> filters = new ArrayList<>();
    private ArrayList<ScoreFunctionBuilder> scoreFunctions = new ArrayList<>();
    private Float minScore = null;
    
    public static final String NAME = "function_score";
    ScoreFunctionParserMapper functionParserMapper;
    // For better readability of error message
    static final String MISPLACED_FUNCTION_MESSAGE_PREFIX = "You can either define \"functions\":[...] or a single function, not both. ";
    static final String MISPLACED_BOOST_FUNCTION_MESSAGE_SUFFIX = " Did you mean \"boost\" instead?";

    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    @Inject
    public FunctionScoreQueryBuilder(ScoreFunctionParserMapper functionParserMapper) {
        this.queryBuilder = null;
        this.filterBuilder = null;
        this.functionParserMapper = functionParserMapper;
    }

    /**
     * Creates a function_score query that executes on documents that match query a query.
     * Query and filter will be wrapped into a filtered_query.
     *
     * @param queryBuilder the query that defines which documents the function_score query will be executed on.
     */
    public FunctionScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.filterBuilder = null;
    }

    /**
     * Creates a function_score query that executes on documents that match query a query.
     * Query and filter will be wrapped into a filtered_query.
     *
     * @param filterBuilder the filter that defines which documents the function_score query will be executed on.
     */
    public FunctionScoreQueryBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        this.queryBuilder = null;
    }

    /**
     * Creates a function_score query that executes on documents that match query and filter.
     * Query and filter will be wrapped into a filtered_query.
     *
     * @param queryBuilder a query that will; be wrapped in a filtered query.
     * @param filterBuilder the filter for the filtered query.
     */
    public FunctionScoreQueryBuilder(QueryBuilder queryBuilder, FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        this.queryBuilder = queryBuilder;
    }

    public FunctionScoreQueryBuilder() {
        this.filterBuilder = null;
        this.queryBuilder = null;
    }

    /**
     * Creates a function_score query that will execute the function scoreFunctionBuilder on all documents.
     *
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder(ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        queryBuilder = null;
        filterBuilder = null;
        this.filters.add(null);
        this.scoreFunctions.add(scoreFunctionBuilder);
    }

    /**
     * Adds a score function that will will execute the function scoreFunctionBuilder on all documents matching the filter.
     *
     * @param filter the filter that defines which documents the function_score query will be executed on.
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder add(FilterBuilder filter, ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        this.filters.add(filter);
        this.scoreFunctions.add(scoreFunctionBuilder);
        return this;
    }

    /**
     * Adds a score function that will will execute the function scoreFunctionBuilder on all documents.
     *
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder add(ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        this.filters.add(null);
        this.scoreFunctions.add(scoreFunctionBuilder);
        return this;
    }

    /**
     * Score mode defines how results of individual score functions will be aggregated.
     * Can be first, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder scoreMode(String scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Score mode defines how the combined result of score functions will influence the final score together with the sub query score.
     * Can be replace, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder boostMode(String boostMode) {
        this.boostMode = boostMode;
        return this;
    }

    /**
     * Score mode defines how the combined result of score functions will influence the final score together with the sub query score.
     */
    public FunctionScoreQueryBuilder boostMode(CombineFunction combineFunction) {
        this.boostMode = combineFunction.getName();
        return this;
    }

    /**
     * Tha maximum boost that will be applied by function score.
     */
    public FunctionScoreQueryBuilder maxBoost(float maxBoost) {
        this.maxBoost = maxBoost;
        return this;
    }

    /**
     * Sets the boost for this query. Documents matching this query will (in
     * addition to the normal weightings) have their score multiplied by the
     * boost provided.
     */
    @Override
    public FunctionScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FunctionScoreQueryBuilder.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        builder.startArray("functions");
        for (int i = 0; i < filters.size(); i++) {
            builder.startObject();
            if (filters.get(i) != null) {
                builder.field("filter");
                filters.get(i).toXContent(builder, params);
            }
            scoreFunctions.get(i).toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();

        if (scoreMode != null) {
            builder.field("score_mode", scoreMode);
        }
        if (boostMode != null) {
            builder.field("boost_mode", boostMode);
        }
        if (maxBoost != null) {
            builder.field("max_boost", maxBoost);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (minScore != null) {
            builder.field("min_score", minScore);
        }

        builder.endObject();
    }

    public FunctionScoreQueryBuilder setMinScore(float minScore) {
        this.minScore = minScore;
        return this;
    }
    
    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    private static final ImmutableMap<String, CombineFunction> combineFunctionsMap;

    static {
        CombineFunction[] values = CombineFunction.values();
        Builder<String, CombineFunction> combineFunctionMapBuilder = ImmutableMap.<String, CombineFunction>builder();
        for (CombineFunction combineFunction : values) {
            combineFunctionMapBuilder.put(combineFunction.getName(), combineFunction);
        }
        combineFunctionsMap = combineFunctionMapBuilder.build();
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        Filter filter = null;
        float boost = 1.0f;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = FiltersFunctionScoreQuery.ScoreMode.Multiply;
        ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions = new ArrayList<>();
        float maxBoost = Float.MAX_VALUE;
        Float minScore = null;

        String currentFieldName = null;
        XContentParser.Token token;
        CombineFunction combineFunction = CombineFunction.MULT;
        // Either define array of functions and filters or only one function
        boolean functionArrayFound = false;
        boolean singleFunctionFound = false;
        String singleFunctionName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("query".equals(currentFieldName)) {
                query = parseContext.parseInnerQuery();
            } else if ("filter".equals(currentFieldName)) {
                filter = parseContext.parseInnerFilter();
            } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                scoreMode = parseScoreMode(parseContext, parser);
            } else if ("boost_mode".equals(currentFieldName) || "boostMode".equals(currentFieldName)) {
                combineFunction = parseBoostMode(parseContext, parser);
            } else if ("max_boost".equals(currentFieldName) || "maxBoost".equals(currentFieldName)) {
                maxBoost = parser.floatValue();
            } else if ("boost".equals(currentFieldName)) {
                boost = parser.floatValue();
            } else if ("min_score".equals(currentFieldName) || "minScore".equals(currentFieldName)) {
                minScore = parser.floatValue();
            } else if ("functions".equals(currentFieldName)) {
                if (singleFunctionFound) {
                    String errorString = "Found \"" + singleFunctionName + "\" already, now encountering \"functions\": [...].";
                    handleMisplacedFunctionsDeclaration(errorString, singleFunctionName);
                }
                currentFieldName = parseFiltersAndFunctions(parseContext, parser, filterFunctions, currentFieldName);
                functionArrayFound = true;
            } else {
                ScoreFunction scoreFunction;
                if (currentFieldName.equals("weight")) {
                    scoreFunction = new WeightFactorFunction(parser.floatValue());

                } else {
                    // we try to parse a score function. If there is no score
                    // function for the current field name,
                    // functionParserMapper.get() will throw an Exception.
                    scoreFunction = functionParserMapper.get(parseContext.index(), currentFieldName).parse(parseContext, parser);
                }
                if (functionArrayFound) {
                    String errorString = "Found \"functions\": [...] already, now encountering \"" + currentFieldName + "\".";
                    handleMisplacedFunctionsDeclaration(errorString, currentFieldName);
                }
                if (filterFunctions.size() > 0) {
                    String errorString = "Found function " + singleFunctionName + " already, now encountering \"" + currentFieldName + "\". Use functions[{...},...] if you want to define several functions.";
                    throw new ElasticsearchParseException(errorString);
                }
                filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(null, scoreFunction));
                singleFunctionFound = true;
                singleFunctionName = currentFieldName;
            }
        }
        if (query == null && filter == null) {
            query = Queries.newMatchAllQuery();
        } else if (query == null && filter != null) {
            query = new ConstantScoreQuery(filter);
        } else if (query != null && filter != null) {
            query = new FilteredQuery(query, filter);
        }
        // if all filter elements returned null, just use the query
        if (filterFunctions.isEmpty()) {
            return query;
        }
        // handle cases where only one score function and no filter was
        // provided. In this case we create a FunctionScoreQuery.
        if (filterFunctions.size() == 1 && (filterFunctions.get(0).filter == null || filterFunctions.get(0).filter instanceof MatchAllDocsFilter)) {
            FunctionScoreQuery theQuery = new FunctionScoreQuery(query, filterFunctions.get(0).function, minScore);
            if (combineFunction != null) {
                theQuery.setCombineFunction(combineFunction);
            }
            theQuery.setBoost(boost);
            theQuery.setMaxBoost(maxBoost);
            return theQuery;
            // in all other cases we create a FiltersFunctionScoreQuery.
        } else {
            FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, scoreMode,
                    filterFunctions.toArray(new FiltersFunctionScoreQuery.FilterFunction[filterFunctions.size()]), maxBoost, minScore);
            if (combineFunction != null) {
                functionScoreQuery.setCombineFunction(combineFunction);
            }
            functionScoreQuery.setBoost(boost);
            return functionScoreQuery;
        }
    }

    private void handleMisplacedFunctionsDeclaration(String errorString, String functionName) {
        errorString = MISPLACED_FUNCTION_MESSAGE_PREFIX + errorString;
        if (Arrays.asList(FactorParser.NAMES).contains(functionName)) {
            errorString = errorString + MISPLACED_BOOST_FUNCTION_MESSAGE_SUFFIX;
        }
        throw new ElasticsearchParseException(errorString);
    }

    private String parseFiltersAndFunctions(QueryParseContext parseContext, XContentParser parser,
                                            ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions, String currentFieldName) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            Filter filter = null;
            ScoreFunction scoreFunction = null;
            Float functionWeight = null;
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(parseContext.index(), NAME + ": malformed query, expected a "
                        + XContentParser.Token.START_OBJECT + " while parsing functions but got a " + token);
            } else {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (WEIGHT_FIELD.match(currentFieldName)) {
                        functionWeight = parser.floatValue();
                    } else {
                        if ("filter".equals(currentFieldName)) {
                            filter = parseContext.parseInnerFilter();
                        } else {
                            // do not need to check null here,
                            // functionParserMapper throws exception if parser
                            // non-existent
                            ScoreFunctionParser functionParser = functionParserMapper.get(parseContext.index(), currentFieldName);
                            scoreFunction = functionParser.parse(parseContext, parser);
                        }
                    }
                }
                if (functionWeight != null) {
                    scoreFunction = new WeightFactorFunction(functionWeight, scoreFunction);
                }
            }
            if (filter == null) {
                filter = Queries.MATCH_ALL_FILTER;
            }
            if (scoreFunction == null) {
                throw new ElasticsearchParseException("function_score: One entry in functions list is missing a function.");
            }
            filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(filter, scoreFunction));

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
            throw new QueryParsingException(parseContext.index(), NAME + " illegal score_mode [" + scoreMode + "]");
        }
    }

    private CombineFunction parseBoostMode(QueryParseContext parseContext, XContentParser parser) throws IOException {
        String boostMode = parser.text();
        CombineFunction cf = combineFunctionsMap.get(boostMode);
        if (cf == null) {
            throw new QueryParsingException(parseContext.index(), NAME + " illegal boost_mode [" + boostMode + "]");
        }
        return cf;
    }
}