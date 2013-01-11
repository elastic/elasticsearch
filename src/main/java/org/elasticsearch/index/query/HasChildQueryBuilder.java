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

    private String scope;

    private float boost = 1.0f;

    private String scoreType;

    private String executionType;

    public HasChildQueryBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
    }

    /**
     * The scope of the query, which can later be used, for example, to run facets against the child docs that
     * matches the query.
     */
    public HasChildQueryBuilder scope(String scope) {
        this.scope = scope;
        return this;
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
     * Expert: Sets the low level child to parent filtering implementation. Can be: 'bitset' or 'uid'
     * Only applicable when score_type is set to none.
     * <p/>
     * This option is experimental and will be removed.
     */
    public HasChildQueryBuilder executionType(String executionType) {
        this.executionType = executionType;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasChildQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("child_type", childType);
        if (scope != null) {
            builder.field("_scope", scope);
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (scoreType != null) {
            builder.field("score_type", scoreType);
        }
        builder.endObject();
    }
}
