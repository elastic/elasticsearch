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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 */
public class FunctionScoreQueryBuilder extends AbstractQueryBuilder<FunctionScoreQueryBuilder> {
    public static final String NAME = "function_score";

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

    public static final CombineFunction DEFAULT_BOOST_MODE = CombineFunction.MULTIPLY;
    public static final FunctionScoreQuery.ScoreMode DEFAULT_SCORE_MODE = FunctionScoreQuery.ScoreMode.MULTIPLY;

    private final QueryBuilder query;

    private float maxBoost = FunctionScoreQuery.DEFAULT_MAX_BOOST;

    private FunctionScoreQuery.ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private CombineFunction boostMode;

    private Float minScore = null;

    private final FilterFunctionBuilder[] filterFunctionBuilders;

    /**
     * Creates a function_score query without functions
     *
     * @param query the query that needs to be custom scored
     */
    public FunctionScoreQueryBuilder(QueryBuilder query) {
        this(query, new FilterFunctionBuilder[0]);
    }

    /**
     * Creates a function_score query that executes the provided filters and functions on all documents
     *
     * @param filterFunctionBuilders the filters and functions
     */
    public FunctionScoreQueryBuilder(FilterFunctionBuilder[] filterFunctionBuilders) {
        this(new MatchAllQueryBuilder(), filterFunctionBuilders);
    }

    /**
     * Creates a function_score query that will execute the function provided on all documents
     *
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder(ScoreFunctionBuilder<?> scoreFunctionBuilder) {
        this(new MatchAllQueryBuilder(), new FilterFunctionBuilder[]{new FilterFunctionBuilder(scoreFunctionBuilder)});
    }

    /**
     * Creates a function_score query that will execute the function provided in the context of the provided query
     *
     * @param query the query to custom score
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder(QueryBuilder query, ScoreFunctionBuilder<?> scoreFunctionBuilder) {
        this(query, new FilterFunctionBuilder[]{new FilterFunctionBuilder(scoreFunctionBuilder)});
    }

    /**
     * Creates a function_score query that executes the provided filters and functions on documents that match a query.
     *
     * @param query the query that defines which documents the function_score query will be executed on.
     * @param filterFunctionBuilders the filters and functions
     */
    public FunctionScoreQueryBuilder(QueryBuilder query, FilterFunctionBuilder[] filterFunctionBuilders) {
        if (query == null) {
            throw new IllegalArgumentException("function_score: query must not be null");
        }
        if (filterFunctionBuilders == null) {
            throw new IllegalArgumentException("function_score: filters and functions array must not be null");
        }
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            if (filterFunctionBuilder == null) {
                throw new IllegalArgumentException("function_score: each filter and function must not be null");
            }
        }
        this.query = query;
        this.filterFunctionBuilders = filterFunctionBuilders;
    }

    /**
     * Read from a stream.
     */
    public FunctionScoreQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        filterFunctionBuilders = in.readList(FilterFunctionBuilder::new).toArray(new FilterFunctionBuilder[0]);
        maxBoost = in.readFloat();
        minScore = in.readOptionalFloat();
        boostMode = in.readOptionalWriteable(CombineFunction::readFromStream);
        scoreMode = FunctionScoreQuery.ScoreMode.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
        out.writeList(Arrays.asList(filterFunctionBuilders));
        out.writeFloat(maxBoost);
        out.writeOptionalFloat(minScore);
        out.writeOptionalWriteable(boostMode);
        scoreMode.writeTo(out);
    }

    /**
     * Returns the query that defines which documents the function_score query will be executed on.
     */
    public QueryBuilder query() {
        return this.query;
    }

    /**
     * Returns the filters and functions
     */
    public FilterFunctionBuilder[] filterFunctionBuilders() {
        return this.filterFunctionBuilders;
    }

    /**
     * Score mode defines how results of individual score functions will be aggregated.
     * @see FunctionScoreQuery.ScoreMode
     */
    public FunctionScoreQueryBuilder scoreMode(FunctionScoreQuery.ScoreMode scoreMode) {
        if (scoreMode == null) {
            throw new IllegalArgumentException("[" + NAME + "]  requires 'score_mode' field");
        }
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Returns the score mode, meaning how results of individual score functions will be aggregated.
     * @see FunctionScoreQuery.ScoreMode
     */
    public FunctionScoreQuery.ScoreMode scoreMode() {
        return this.scoreMode;
    }

    /**
     * Boost mode defines how the combined result of score functions will influence the final score together with the sub query score.
     * @see CombineFunction
     */
    public FunctionScoreQueryBuilder boostMode(CombineFunction combineFunction) {
        if (combineFunction == null) {
            throw new IllegalArgumentException("[" + NAME + "]  requires 'boost_mode' field");
        }
        this.boostMode = combineFunction;
        return this;
    }

    /**
     * Returns the boost mode, meaning how the combined result of score functions will influence the final score together with the sub query
     * score.
     *
     * @see CombineFunction
     */
    public CombineFunction boostMode() {
        return this.boostMode;
    }

    /**
     * Sets the maximum boost that will be applied by function score.
     */
    public FunctionScoreQueryBuilder maxBoost(float maxBoost) {
        this.maxBoost = maxBoost;
        return this;
    }

    /**
     * Returns the maximum boost that will be applied by function score.
     */
    public float maxBoost() {
        return this.maxBoost;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName());
            query.toXContent(builder, params);
        }
        builder.startArray(FUNCTIONS_FIELD.getPreferredName());
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            filterFunctionBuilder.toXContent(builder, params);
        }
        builder.endArray();

        builder.field(SCORE_MODE_FIELD.getPreferredName(), scoreMode.name().toLowerCase(Locale.ROOT));
        if (boostMode != null) {
            builder.field(BOOST_MODE_FIELD.getPreferredName(), boostMode.name().toLowerCase(Locale.ROOT));
        }
        builder.field(MAX_BOOST_FIELD.getPreferredName(), maxBoost);
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public FunctionScoreQueryBuilder setMinScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    public Float getMinScore() {
        return this.minScore;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(FunctionScoreQueryBuilder other) {
        return Objects.equals(this.query, other.query) &&
                Arrays.equals(this.filterFunctionBuilders, other.filterFunctionBuilders) &&
                Objects.equals(this.boostMode, other.boostMode) &&
                Objects.equals(this.scoreMode, other.scoreMode) &&
                Objects.equals(this.minScore, other.minScore) &&
                Objects.equals(this.maxBoost, other.maxBoost);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.query, Arrays.hashCode(this.filterFunctionBuilders), this.boostMode, this.scoreMode, this.minScore,
                this.maxBoost);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ScoreFunction[] filterFunctions = new ScoreFunction[filterFunctionBuilders.length];
        int i = 0;
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            ScoreFunction scoreFunction = filterFunctionBuilder.getScoreFunction().toFunction(context);
            if (filterFunctionBuilder.getFilter().getName().equals(MatchAllQueryBuilder.NAME)) {
                filterFunctions[i++] = scoreFunction;
            } else {
                Query filter = filterFunctionBuilder.getFilter().toQuery(context);
                filterFunctions[i++] = new FunctionScoreQuery.FilterScoreFunction(filter, scoreFunction);
            }
        }

        Query query = this.query.toQuery(context);
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        CombineFunction boostMode = this.boostMode == null ? DEFAULT_BOOST_MODE : this.boostMode;
        // handle cases where only one score function and no filter was provided. In this case we create a FunctionScoreQuery.
        if (filterFunctions.length == 0) {
            return new FunctionScoreQuery(query, minScore, maxBoost);
        } else if (filterFunctions.length == 1 && filterFunctions[0] instanceof FunctionScoreQuery.FilterScoreFunction == false) {
            return new FunctionScoreQuery(query, filterFunctions[0], boostMode, minScore, maxBoost);
        }
        // in all other cases we create a FunctionScoreQuery with filters
        return new FunctionScoreQuery(query, scoreMode, filterFunctions, boostMode, minScore, maxBoost);
    }

    /**
     * Function to be associated with an optional filter, meaning it will be executed only for the documents
     * that match the given filter.
     */
    public static class FilterFunctionBuilder implements ToXContentObject, Writeable {
        private final QueryBuilder filter;
        private final ScoreFunctionBuilder<?> scoreFunction;

        public FilterFunctionBuilder(ScoreFunctionBuilder<?> scoreFunctionBuilder) {
            this(new MatchAllQueryBuilder(), scoreFunctionBuilder);
        }

        public FilterFunctionBuilder(QueryBuilder filter, ScoreFunctionBuilder<?> scoreFunction) {
            if (filter == null) {
                throw new IllegalArgumentException("function_score: filter must not be null");
            }
            if (scoreFunction == null) {
                throw new IllegalArgumentException("function_score: function must not be null");
            }
            this.filter = filter;
            this.scoreFunction = scoreFunction;
        }

        /**
         * Read from a stream.
         */
        public FilterFunctionBuilder(StreamInput in) throws IOException {
            filter = in.readNamedWriteable(QueryBuilder.class);
            scoreFunction = in.readNamedWriteable(ScoreFunctionBuilder.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(filter);
            out.writeNamedWriteable(scoreFunction);
        }

        public QueryBuilder getFilter() {
            return filter;
        }

        public ScoreFunctionBuilder<?> getScoreFunction() {
            return scoreFunction;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FILTER_FIELD.getPreferredName());
            filter.toXContent(builder, params);
            scoreFunction.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter, scoreFunction);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FilterFunctionBuilder that = (FilterFunctionBuilder) obj;
            return Objects.equals(this.filter, that.filter) &&
                    Objects.equals(this.scoreFunction, that.scoreFunction);
        }

        public FilterFunctionBuilder rewrite(QueryRewriteContext context) throws IOException {
            QueryBuilder rewrite = filter.rewrite(context);
            if (rewrite != filter) {
                return new FilterFunctionBuilder(rewrite, scoreFunction);
            }
            return this;
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder queryBuilder = this.query.rewrite(queryRewriteContext);
        if (queryBuilder instanceof MatchNoneQueryBuilder) {
            return queryBuilder;
        }

        FilterFunctionBuilder[] rewrittenBuilders = new FilterFunctionBuilder[this.filterFunctionBuilders.length];
        boolean rewritten = false;
        for (int i = 0; i < rewrittenBuilders.length; i++) {
            FilterFunctionBuilder rewrite = filterFunctionBuilders[i].rewrite(queryRewriteContext);
            rewritten |= rewrite != filterFunctionBuilders[i];
            rewrittenBuilders[i] = rewrite;
        }
        if (queryBuilder != query || rewritten) {
            FunctionScoreQueryBuilder newQueryBuilder = new FunctionScoreQueryBuilder(queryBuilder, rewrittenBuilders);
            newQueryBuilder.scoreMode = scoreMode;
            newQueryBuilder.minScore = minScore;
            newQueryBuilder.maxBoost = maxBoost;
            newQueryBuilder.boostMode = boostMode;
            return newQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        InnerHitContextBuilder.extractInnerHits(query(), innerHits);
    }

    public static FunctionScoreQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        FunctionScoreQuery.ScoreMode scoreMode = FunctionScoreQueryBuilder.DEFAULT_SCORE_MODE;
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
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (query != null) {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. [query] is already defined.",
                                NAME);
                    }
                    query = parseInnerQueryBuilder(parser);
                } else {
                    if (singleFunctionFound) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "failed to parse [{}] query. already found function [{}], now encountering [{}]. use [functions] "
                                        + "array if you want to define several functions.",
                                NAME, singleFunctionName, currentFieldName);
                    }
                    if (functionArrayFound) {
                        String errorString = "already found [functions] array, now encountering [" + currentFieldName + "].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    singleFunctionFound = true;
                    singleFunctionName = currentFieldName;

                        ScoreFunctionBuilder<?> scoreFunction = parser.namedObject(ScoreFunctionBuilder.class, currentFieldName,
                                null);
                    filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FUNCTIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (singleFunctionFound) {
                        String errorString = "already found [" + singleFunctionName + "], now encountering [functions].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    functionArrayFound = true;
                    currentFieldName = parseFiltersAndFunctions(parser, filterFunctionBuilders);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. array [{}] is not supported",
                            NAME, currentFieldName);
                }

            } else if (token.isValue()) {
                if (SCORE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scoreMode = FunctionScoreQuery.ScoreMode.fromString(parser.text());
                } else if (BOOST_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    combineFunction = CombineFunction.fromString(parser.text());
                } else if (MAX_BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxBoost = parser.floatValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (MIN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minScore = parser.floatValue();
                } else {
                    if (singleFunctionFound) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "failed to parse [{}] query. already found function [{}], now encountering [{}]. use [functions] array "
                                        + "if you want to define several functions.",
                                NAME, singleFunctionName, currentFieldName);
                    }
                    if (functionArrayFound) {
                        String errorString = "already found [functions] array, now encountering [" + currentFieldName + "].";
                        handleMisplacedFunctionsDeclaration(parser.getTokenLocation(), errorString);
                    }
                    if (WEIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        filterFunctionBuilders.add(
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(new WeightBuilder().setWeight(parser.floatValue())));
                        singleFunctionFound = true;
                        singleFunctionName = currentFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported",
                                NAME, currentFieldName);
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
        throw new ParsingException(contentLocation, "failed to parse [{}] query. [{}]", NAME,
                MISPLACED_FUNCTION_MESSAGE_PREFIX + errorString);
    }

    private static String parseFiltersAndFunctions(XContentParser parser,
            List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            QueryBuilder filter = null;
            ScoreFunctionBuilder<?> scoreFunction = null;
            Float functionWeight = null;
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                        "failed to parse [{}]. malformed query, expected a [{}] while parsing functions but got a [{}] instead",
                        XContentParser.Token.START_OBJECT, token, NAME);
            } else {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            filter = parseInnerQueryBuilder(parser);
                        } else {
                            if (scoreFunction != null) {
                                throw new ParsingException(parser.getTokenLocation(),
                                        "failed to parse function_score functions. already found [{}], now encountering [{}].",
                                        scoreFunction.getName(), currentFieldName);
                            }
                            scoreFunction = parser.namedObject(ScoreFunctionBuilder.class, currentFieldName, null);
                        }
                    } else if (token.isValue()) {
                        if (WEIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            functionWeight = parser.floatValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported",
                                    NAME, currentFieldName);
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
                throw new ParsingException(parser.getTokenLocation(),
                        "failed to parse [{}] query. an entry in functions list is missing a function.", NAME);
            }
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(filter, scoreFunction));
        }
        return currentFieldName;
    }
}
