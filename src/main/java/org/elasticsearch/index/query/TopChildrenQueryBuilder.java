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
 *
 */
public class TopChildrenQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<TopChildrenQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private String childType;

    private String score;

    private float boost = 1.0f;

    private int factor = -1;

    private int incrementalFactor = -1;

    private String queryName;

    public TopChildrenQueryBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
    }

    /**
     * How to compute the score. Possible values are: <tt>max</tt>, <tt>sum</tt>, or <tt>avg</tt>. Defaults
     * to <tt>max</tt>.
     */
    public TopChildrenQueryBuilder score(String score) {
        this.score = score;
        return this;
    }

    /**
     * Controls the multiplication factor of the initial hits required from the child query over the main query request.
     * Defaults to 5.
     */
    public TopChildrenQueryBuilder factor(int factor) {
        this.factor = factor;
        return this;
    }

    /**
     * Sets the incremental factor when the query needs to be re-run in order to fetch more results. Defaults to 2.
     */
    public TopChildrenQueryBuilder incrementalFactor(int incrementalFactor) {
        this.incrementalFactor = incrementalFactor;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public TopChildrenQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public TopChildrenQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TopChildrenQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("type", childType);
        if (score != null) {
            builder.field("score", score);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (factor != -1) {
            builder.field("factor", factor);
        }
        if (incrementalFactor != -1) {
            builder.field("incremental_factor", incrementalFactor);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}
