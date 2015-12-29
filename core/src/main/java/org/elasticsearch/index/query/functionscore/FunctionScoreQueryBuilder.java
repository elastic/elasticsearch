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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.EmptyQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 */
public class FunctionScoreQueryBuilder extends AbstractQueryBuilder<FunctionScoreQueryBuilder> {

    public static final String NAME = "function_score";

    public static final CombineFunction DEFAULT_BOOST_MODE = CombineFunction.MULTIPLY;
    public static final FiltersFunctionScoreQuery.ScoreMode DEFAULT_SCORE_MODE = FiltersFunctionScoreQuery.ScoreMode.MULTIPLY;

    private final QueryBuilder<?> query;

    private float maxBoost = FunctionScoreQuery.DEFAULT_MAX_BOOST;

    private FiltersFunctionScoreQuery.ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private CombineFunction boostMode;

    private Float minScore = null;

    private final FilterFunctionBuilder[] filterFunctionBuilders;

    /**
     * Creates a function_score query without functions
     *
     * @param query the query that needs to be custom scored
     */
    public FunctionScoreQueryBuilder(QueryBuilder<?> query) {
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
    public FunctionScoreQueryBuilder(ScoreFunctionBuilder scoreFunctionBuilder) {
        this(new MatchAllQueryBuilder(), new FilterFunctionBuilder[]{new FilterFunctionBuilder(scoreFunctionBuilder)});
    }

    /**
     * Creates a function_score query that will execute the function provided in the context of the provided query
     *
     * @param query the query to custom score
     * @param scoreFunctionBuilder score function that is executed
     */
    public FunctionScoreQueryBuilder(QueryBuilder<?> query, ScoreFunctionBuilder scoreFunctionBuilder) {
        this(query, new FilterFunctionBuilder[]{new FilterFunctionBuilder(scoreFunctionBuilder)});
    }

    /**
     * Creates a function_score query that executes the provided filters and functions on documents that match a query.
     *
     * @param query the query that defines which documents the function_score query will be executed on.
     * @param filterFunctionBuilders the filters and functions
     */
    public FunctionScoreQueryBuilder(QueryBuilder<?> query, FilterFunctionBuilder[] filterFunctionBuilders) {
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
     * Returns the query that defines which documents the function_score query will be executed on.
     */
    public QueryBuilder<?> query() {
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
     * @see org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode
     */
    public FunctionScoreQueryBuilder scoreMode(FiltersFunctionScoreQuery.ScoreMode scoreMode) {
        if (scoreMode == null) {
            throw new IllegalArgumentException("[" + NAME + "]  requires 'score_mode' field");
        }
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Returns the score mode, meaning how results of individual score functions will be aggregated.
     * @see org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode
     */
    public FiltersFunctionScoreQuery.ScoreMode scoreMode() {
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
     * Returns the boost mode, meaning how the combined result of score functions will influence the final score together with the sub query score.
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
            builder.field("query");
            query.toXContent(builder, params);
        }
        builder.startArray("functions");
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            filterFunctionBuilder.toXContent(builder, params);
        }
        builder.endArray();

        builder.field("score_mode", scoreMode.name().toLowerCase(Locale.ROOT));
        if (boostMode != null) {
            builder.field("boost_mode", boostMode.name().toLowerCase(Locale.ROOT));
        }
        builder.field("max_boost", maxBoost);
        if (minScore != null) {
            builder.field("min_score", minScore);
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
        return FunctionScoreQueryBuilder.NAME;
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
        return Objects.hash(this.query, Arrays.hashCode(this.filterFunctionBuilders), this.boostMode, this.scoreMode, this.minScore, this.maxBoost);
    }

    @Override
    protected FunctionScoreQueryBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder<?> query = in.readQuery();
        int size = in.readVInt();
        FilterFunctionBuilder[] filterFunctionBuilders = new FilterFunctionBuilder[size];
        for (int i = 0; i < size; i++) {
            filterFunctionBuilders[i] = FilterFunctionBuilder.PROTOTYPE.readFrom(in);
        }
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(query, filterFunctionBuilders);
        functionScoreQueryBuilder.maxBoost(in.readFloat());
        if (in.readBoolean()) {
            functionScoreQueryBuilder.setMinScore(in.readFloat());
        }
        if (in.readBoolean()) {
            functionScoreQueryBuilder.boostMode(CombineFunction.readCombineFunctionFrom(in));
        }
        functionScoreQueryBuilder.scoreMode(FiltersFunctionScoreQuery.ScoreMode.readScoreModeFrom(in));
        return functionScoreQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(query);
        out.writeVInt(filterFunctionBuilders.length);
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            filterFunctionBuilder.writeTo(out);
        }
        out.writeFloat(maxBoost);
        if (minScore == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloat(minScore);
        }
        if (boostMode == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            boostMode.writeTo(out);
        }
        scoreMode.writeTo(out);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        FiltersFunctionScoreQuery.FilterFunction[] filterFunctions = new FiltersFunctionScoreQuery.FilterFunction[filterFunctionBuilders.length];
        int i = 0;
        for (FilterFunctionBuilder filterFunctionBuilder : filterFunctionBuilders) {
            Query filter = filterFunctionBuilder.getFilter().toQuery(context);
            ScoreFunction scoreFunction = filterFunctionBuilder.getScoreFunction().toFunction(context);
            filterFunctions[i++] = new FiltersFunctionScoreQuery.FilterFunction(filter, scoreFunction);
        }

        Query query = this.query.toQuery(context);
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        // handle cases where only one score function and no filter was provided. In this case we create a FunctionScoreQuery.
        if (filterFunctions.length == 0 || filterFunctions.length == 1 && (this.filterFunctionBuilders[0].getFilter().getName().equals(MatchAllQueryBuilder.NAME))) {
            ScoreFunction function = filterFunctions.length == 0 ? null : filterFunctions[0].function;
            CombineFunction combineFunction = this.boostMode;
            if (combineFunction == null) {
                if (function != null) {
                    combineFunction = function.getDefaultScoreCombiner();
                } else {
                    combineFunction = DEFAULT_BOOST_MODE;
                }
            }
            return new FunctionScoreQuery(query, function, minScore, combineFunction, maxBoost);
        }
        // in all other cases we create a FiltersFunctionScoreQuery
        return new FiltersFunctionScoreQuery(query, scoreMode, filterFunctions, maxBoost, minScore, boostMode == null ? DEFAULT_BOOST_MODE : boostMode);
    }

    /**
     * Function to be associated with an optional filter, meaning it will be executed only for the documents
     * that match the given filter.
     */
    public static class FilterFunctionBuilder implements ToXContent, Writeable<FilterFunctionBuilder> {
        private static final FilterFunctionBuilder PROTOTYPE = new FilterFunctionBuilder(EmptyQueryBuilder.PROTOTYPE, new RandomScoreFunctionBuilder());

        private final QueryBuilder<?> filter;
        private final ScoreFunctionBuilder scoreFunction;

        public FilterFunctionBuilder(ScoreFunctionBuilder scoreFunctionBuilder) {
            this(new MatchAllQueryBuilder(), scoreFunctionBuilder);
        }

        public FilterFunctionBuilder(QueryBuilder<?> filter, ScoreFunctionBuilder scoreFunction) {
            if (filter == null) {
                throw new IllegalArgumentException("function_score: filter must not be null");
            }
            if (scoreFunction == null) {
                throw new IllegalArgumentException("function_score: function must not be null");
            }
            this.filter = filter;
            this.scoreFunction = scoreFunction;
        }

        public QueryBuilder<?> getFilter() {
            return filter;
        }

        public ScoreFunctionBuilder<?> getScoreFunction() {
            return scoreFunction;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("filter");
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeQuery(filter);
            out.writeScoreFunction(scoreFunction);
        }

        @Override
        public FilterFunctionBuilder readFrom(StreamInput in) throws IOException {
            return new FilterFunctionBuilder(in.readQuery(), in.readScoreFunction());
        }
    }
}
