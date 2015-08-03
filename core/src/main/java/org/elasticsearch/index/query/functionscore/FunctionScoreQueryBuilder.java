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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 */
public class FunctionScoreQueryBuilder extends AbstractQueryBuilder<FunctionScoreQueryBuilder> {

    public static final String NAME = "function_score";

    public static final FiltersFunctionScoreQuery.ScoreMode DEFAULT_SCORE_MODE = FiltersFunctionScoreQuery.ScoreMode.Multiply;

    public static final CombineFunction DEFAULT_BOOST_MODE = CombineFunction.MULT;

    public static final QueryBuilder DEFAULT_FUNCTION_FILTER = QueryBuilders.matchAllQuery();

    private final QueryBuilder queryBuilder;

    private final QueryBuilder filterBuilder;

    private Float maxBoost;

    private FiltersFunctionScoreQuery.ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private CombineFunction boostMode = DEFAULT_BOOST_MODE;

    private List<Tuple<QueryBuilder, ScoreFunctionBuilder>> filterFunctions = new ArrayList<>();

    private Float minScore = null;

    static final FunctionScoreQueryBuilder PROTOTYPE = new FunctionScoreQueryBuilder();

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

    public FunctionScoreQueryBuilder() {
        this.filterBuilder = null;
        this.queryBuilder = null;
    }

    public FunctionScoreQueryBuilder(QueryBuilder queryBuilder, QueryBuilder filter) {
        this.queryBuilder = queryBuilder;
        this.filterBuilder = filter;
    }

    /**
     * Creates a function_score query that will execute the function scoreFunctionBuilder on all documents.
     *
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder(ScoreFunctionBuilder scoreFunctionBuilder) {
        queryBuilder = null;
        filterBuilder = null;
        this.add(scoreFunctionBuilder);
    }

    /**
     * Adds a score function that will will execute the function scoreFunctionBuilder on all documents matching the filter.
     *
     * @param filter the filter that defines which documents the function_score query will be executed on.
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder add(QueryBuilder filter, ScoreFunctionBuilder scoreFunctionBuilder) {
        filter = (filter == null) ? DEFAULT_FUNCTION_FILTER : filter;
        this.filterFunctions.add(new Tuple(filter, scoreFunctionBuilder));
        return this;
    }

    /**
     * Adds a score function that will will execute the function scoreFunctionBuilder on all documents.
     *
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder add(ScoreFunctionBuilder scoreFunctionBuilder) {
        this.filterFunctions.add(new Tuple(DEFAULT_FUNCTION_FILTER, scoreFunctionBuilder));
        return this;
    }

    /**
     * Adds all score functions that will will execute the function scoreFunctionBuilder on all documents matching the filter.
     *
     * @param filterFunctions a list of tuple (filter, score function)
     */
    public FunctionScoreQueryBuilder addAll(List<Tuple<QueryBuilder, ScoreFunctionBuilder>> filterFunctions) {
        for (Tuple<QueryBuilder, ScoreFunctionBuilder> filterFunction : filterFunctions) {
            this.add(filterFunction.v1(), filterFunction.v2());
        }
        return this;
    }

    /**
     * Returns the score functions with their filters.
     */
    public List<Tuple<QueryBuilder, ScoreFunctionBuilder>> getFilterFunctions() {
        return this.filterFunctions;
    }

    /**
     * Score mode defines how results of individual score functions will be aggregated.
     * Can be first, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder scoreMode(String scoreMode) {
        try {
            this.scoreMode = getScoreModeFunction(scoreMode);
        } catch (Exception e) {
            this.scoreMode = null; // we report this error in validate
        }
        return this;
    }

    /**
     * Score mode defines how results of individual score functions will be aggregated.
     * Can be first, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder scoreMode(FiltersFunctionScoreQuery.ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Score mode defines how the combined result of score functions will influence the final score together with the sub query score.
     * Can be replace, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder boostMode(String boostMode) {
        try {
            this.boostMode = getCombineFunction(boostMode);
        } catch (Exception e) {
            this.boostMode = null; // we report this error in validate
        }
        return this;
    }

    /**
     * Score mode defines how the combined result of score functions will influence the final score together with the sub query score.
     * Can be replace, avg, max, sum, min, multiply
     */
    public FunctionScoreQueryBuilder boostMode(CombineFunction boostMode) {
        this.boostMode = boostMode;
        return this;
    }

    /**
     * The maximum boost that will be applied by function score.
     */
    public FunctionScoreQueryBuilder maxBoost(Float maxBoost) {
        this.maxBoost = maxBoost;
        return this;
    }

    /*
     * Exclude documents that do not meet a certain score threshold
     */
    public FunctionScoreQueryBuilder minScore(Float minScore) {
        this.minScore = minScore;
        return this;
    }

    /*
     * Exclude documents that do not meet a certain score threshold
     *
     * @deprecated Use {@link #minScore(float)} instead.
     */
    public FunctionScoreQueryBuilder setMinScore(float minScore) {
        return minScore(minScore);
    }

    private static FiltersFunctionScoreQuery.ScoreMode getScoreModeFunction(String scoreMode) throws IOException {
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
            throw new IllegalArgumentException("illegal score mode [ " + scoreMode + "]");
        }
    }

    private static CombineFunction getCombineFunction(String boostMode) throws IOException {
        CombineFunction cf = FunctionScoreQueryParser.combineFunctionsMap.get(boostMode);
        if (cf == null) {
            throw new IllegalArgumentException("illegal boost mode [ " + boostMode + "]");
        }
        return cf;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        builder.startArray("functions");
        for (Tuple<QueryBuilder, ScoreFunctionBuilder> filterFunction : filterFunctions) {
            builder.startObject();
            builder.field("filter");
            filterFunction.v1().toXContent(builder, params);
            filterFunction.v2().toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.field("score_mode", scoreMode.getName());
        builder.field("boost_mode", boostMode.getName());
        if (maxBoost != null) {
            builder.field("max_boost", maxBoost);
        }
        if (minScore != null) {
            builder.field("min_score", minScore);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query;
        if (isEmpty(queryBuilder) && isEmpty(filterBuilder)) {
            query = Queries.newMatchAllQuery();
        } else if (isEmpty(queryBuilder) && !isEmpty(filterBuilder)) {
            query = new ConstantScoreQuery(filterBuilder.toQuery(context));
        } else if (!isEmpty(queryBuilder) && !isEmpty(filterBuilder)) {
            final BooleanQuery filtered = new BooleanQuery();
            filtered.add(queryBuilder.toQuery(context), Occur.MUST);
            filtered.add(filterBuilder.toQuery(context), Occur.FILTER);
            query = filtered;
        } else {
            query = queryBuilder.toQuery(context);
        }
        // if all filter elements returned null, just use the query
        if (filterFunctions.isEmpty() && boostMode == null) {
            return query;
        }
        float maxBoost = (this.maxBoost == null) ? Float.MAX_VALUE : this.maxBoost;

        // we need to convert function builders to actual filter functions
        ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions = new ArrayList<>();
        for (Tuple<QueryBuilder, ScoreFunctionBuilder> filterFunction : this.filterFunctions) {
            filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(
                    filterFunction.v1().toQuery(context),
                    filterFunction.v2().toScoreFunction(context)));
        }

        Query result;
        // handle cases where only one score function and no filter was
        // provided. In this case we create a FunctionScoreQuery.
        if (filterFunctions.size() == 0 || filterFunctions.size() == 1 && (filterFunctions.get(0).filter == null || Queries.isConstantMatchAllQuery(filterFunctions.get(0).filter))) {
            ScoreFunction function = filterFunctions.size() == 0 ? null : filterFunctions.get(0).function;
            FunctionScoreQuery theQuery = new FunctionScoreQuery(query, function, minScore);
            if (boostMode != null) {
                theQuery.setCombineFunction(boostMode);
            }
            theQuery.setMaxBoost(maxBoost);
            result = theQuery;
            // in all other cases we create a FiltersFunctionScoreQuery.
        } else {
            FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, scoreMode,
                    filterFunctions.toArray(new FiltersFunctionScoreQuery.FilterFunction[filterFunctions.size()]), maxBoost, minScore);
            if (boostMode != null) {
                functionScoreQuery.setCombineFunction(boostMode);
            }
            result = functionScoreQuery;
        }
        result.setBoost(boost);
        if (queryName != null) {
            context.addNamedQuery(queryName, query);
        }
        return result;
    }

    private static boolean isEmpty(QueryBuilder queryBuilder) {
        return queryBuilder == null || queryBuilder.equals(EmptyQueryBuilder.PROTOTYPE);
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        // validate inner query and/or filter
        if (this.queryBuilder != null) {
            validationException = validateInnerQuery(this.queryBuilder, validationException);
        }
        if (this.filterBuilder != null) {
            validationException = validateInnerQuery(this.filterBuilder, validationException);
        }
        // validate options
        if (this.scoreMode == null) {
            validationException = addValidationError("score mode must be set to a proper value and cannot be null", validationException);
        }
        if (this.boostMode == null) {
            validationException = addValidationError("boost mode must be set to a proper value and cannot be null", validationException);
        }
        // validate filter functions
        for (Tuple<QueryBuilder, ScoreFunctionBuilder> filterFunction : filterFunctions) {
            // validate the filter
            validationException = validateInnerQuery(filterFunction.v1(), validationException);
            // and the function score builder
            if (filterFunction.v2() == null) {
                validationException = addValidationError("function_score: function must not be null", validationException);
            } else {
                QueryValidationException exception = filterFunction.v2().validate();
                if (exception != null) {
                    validationException = QueryValidationException.addValidationErrors(exception.validationErrors(), validationException);
                }
            }
        }
        return validationException;
    }

    @Override
    protected FunctionScoreQueryBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder queryBuilder = in.readOptionalQuery();
        QueryBuilder filterBuilder = in.readOptionalQuery();
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(queryBuilder, filterBuilder);
        functionScoreQueryBuilder.maxBoost = in.readOptionalFloat();
        functionScoreQueryBuilder.scoreMode = FiltersFunctionScoreQuery.ScoreMode.readScoreModeFrom(in);
        functionScoreQueryBuilder.boostMode = CombineFunction.readCombineFunctionFrom(in);
        readFilterFunctionsFrom(in, functionScoreQueryBuilder);
        functionScoreQueryBuilder.minScore = in.readOptionalFloat();
        return functionScoreQueryBuilder;
    }

    private static void readFilterFunctionsFrom(StreamInput in, FunctionScoreQueryBuilder builder) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            QueryBuilder filter = in.readQuery();
            ScoreFunctionBuilder function = in.readScoreFunction(); // TODO: prototype needs to be registered, see FunctionScoreModule
            builder.add(filter, function);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalQuery(queryBuilder);
        out.writeOptionalQuery(filterBuilder);
        out.writeOptionalFloat(maxBoost);
        scoreMode.writeTo(out);
        boostMode.writeTo(out);
        writeFilterFunctionsTo(out);
        out.writeOptionalFloat(minScore);
    }

    private void writeFilterFunctionsTo(StreamOutput out) throws IOException {
        out.writeVInt(filterFunctions.size());
        for (Tuple<QueryBuilder, ScoreFunctionBuilder> filterFunction : filterFunctions) {
            out.writeQuery(filterFunction.v1());
            out.writeScoreFunction(filterFunction.v2());
        }
    }

    @Override
    protected boolean doEquals(FunctionScoreQueryBuilder other) {
        return Objects.equals(queryBuilder, other.queryBuilder) &&
                Objects.equals(filterBuilder, other.filterBuilder) &&
                Objects.equals(maxBoost, other.maxBoost) &&
                Objects.equals(scoreMode, other.scoreMode) &&
                Objects.equals(boostMode, other.boostMode) &&
                Objects.equals(filterFunctions, other.filterFunctions) &&
                Objects.equals(minScore, other.minScore);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryBuilder, filterBuilder, maxBoost, scoreMode, boostMode, filterFunctions, minScore);
    }
}
