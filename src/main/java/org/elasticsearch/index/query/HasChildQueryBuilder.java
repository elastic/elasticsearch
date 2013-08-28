/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
public class HasChildQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<HasChildQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private String childType;

    private float boost = 1.0f;

    private String scoreType;

    private Integer shortCircuitCutoff;

    private String queryName;

    public HasChildQueryBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public HasChildQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Defines how the scores from the matching child documents are mapped into the parent document.
     */
    public HasChildQueryBuilder scoreType(String scoreType) {
        this.scoreType = scoreType;
        return this;
    }

    /**
     * Configures at what cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs.
     */
    public HasChildQueryBuilder setShortCircuitCutoff(int shortCircuitCutoff) {
        this.shortCircuitCutoff = shortCircuitCutoff;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
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
        if (scoreType != null) {
            builder.field("score_type", scoreType);
        }
        if (shortCircuitCutoff != null) {
            builder.field("short_circuit_cutoff", shortCircuitCutoff);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}
