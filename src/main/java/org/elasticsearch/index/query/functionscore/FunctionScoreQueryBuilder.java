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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 */
public class FunctionScoreQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<FunctionScoreQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private final FilterBuilder filterBuilder;

    private Float boost;

    private Float maxBoost;

    private String scoreMode;
    
    private String boostMode;

    private ArrayList<FilterBuilder> filters = new ArrayList<>();
    private ArrayList<ScoreFunctionBuilder> scoreFunctions = new ArrayList<>();

    public FunctionScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.filterBuilder = null;
    }

    public FunctionScoreQueryBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        this.queryBuilder = null;
    }

    public FunctionScoreQueryBuilder() {
        this.filterBuilder = null;
        this.queryBuilder = null;
    }

    public FunctionScoreQueryBuilder(ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        queryBuilder = null;
        filterBuilder = null;
        this.filters.add(null);
        this.scoreFunctions.add(scoreFunctionBuilder);
    }

    public FunctionScoreQueryBuilder add(FilterBuilder filter, ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        this.filters.add(filter);
        this.scoreFunctions.add(scoreFunctionBuilder);
        return this;
    }

    public FunctionScoreQueryBuilder add(ScoreFunctionBuilder scoreFunctionBuilder) {
        if (scoreFunctionBuilder == null) {
            throw new ElasticsearchIllegalArgumentException("function_score: function must not be null");
        }
        this.filters.add(null);
        this.scoreFunctions.add(scoreFunctionBuilder);
        return this;
    }

    public FunctionScoreQueryBuilder scoreMode(String scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }
    
    public FunctionScoreQueryBuilder boostMode(String boostMode) {
        this.boostMode = boostMode;
        return this;
    }
    
    public FunctionScoreQueryBuilder boostMode(CombineFunction combineFunction) {
        this.boostMode = combineFunction.getName();
        return this;
    }

    public FunctionScoreQueryBuilder maxBoost(float maxBoost) {
        this.maxBoost = maxBoost;
        return this;
    }

    /**
     * Sets the boost for this query. Documents matching this query will (in
     * addition to the normal weightings) have their score multiplied by the
     * boost provided.
     */
    public FunctionScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FunctionScoreQueryParser.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        } 
        // If there is only one function without a filter, we later want to
        // create a FunctionScoreQuery.
        // For this, we only build the scoreFunction.Tthis will be translated to
        // FunctionScoreQuery in the parser.
        if (filters.size() == 1 && filters.get(0) == null) {
            scoreFunctions.get(0).toXContent(builder, params);
        } else { // in all other cases we build the format needed for a
                 // FiltersFunctionScoreQuery
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
        }
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

        builder.endObject();
    }
}