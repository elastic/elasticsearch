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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 *
 *
 */
public class ConstantScoreQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<ConstantScoreQueryBuilder> {

    private final FilterBuilder filterBuilder;
    private final QueryBuilder queryBuilder;

    private float boost = -1;
    

    /**
     * A query that wraps a filter and simply returns a constant score equal to the
     * query boost for every document in the filter.
     *
     * @param filterBuilder The filter to wrap in a constant score query
     */
    public ConstantScoreQueryBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        this.queryBuilder = null;
    }
    /**
     * A query that wraps a query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param queryBuilder The query to wrap in a constant score query
     */
    public ConstantScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.filterBuilder = null;
        this.queryBuilder = queryBuilder;
    }    

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public ConstantScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ConstantScoreQueryParser.NAME);
        if (queryBuilder != null) {
            assert filterBuilder == null;
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);  
        }
        
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}