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
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;

import java.io.IOException;

public class HasChildQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<HasChildQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private String childType;

    private float boost = 1.0f;

    private String scoreMode;

    private Integer minChildren;

    private Integer maxChildren;

    private String queryName;

    private QueryInnerHitBuilder innerHit = null;

    public HasChildQueryBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public HasChildQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Defines how the scores from the matching child documents are mapped into the parent document.
     */
    public HasChildQueryBuilder scoreMode(String scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Defines the minimum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder minChildren(int minChildren) {
        this.minChildren = minChildren;
        return this;
    }

    /**
     * Defines the maximum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder maxChildren(int maxChildren) {
        this.maxChildren = maxChildren;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public HasChildQueryBuilder innerHit(QueryInnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasChildQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("child_type", childType);
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (scoreMode != null) {
            builder.field("score_mode", scoreMode);
        }
        if (minChildren != null) {
            builder.field("min_children", minChildren);
        }
        if (maxChildren != null) {
            builder.field("max_children", maxChildren);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (innerHit != null) {
            builder.startObject("inner_hits");
            builder.value(innerHit);
            builder.endObject();
        }
        builder.endObject();
    }
}
