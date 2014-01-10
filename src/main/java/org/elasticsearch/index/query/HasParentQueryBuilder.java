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
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<HasParentQueryBuilder> {

    private final QueryBuilder queryBuilder;
    private final String parentType;
    private String scoreType;
    private float boost = 1.0f;
    private String queryName;

    /**
     * @param parentType  The parent type
     * @param parentQuery The query that will be matched with parent documents
     */
    public HasParentQueryBuilder(String parentType, QueryBuilder parentQuery) {
        this.parentType = parentType;
        this.queryBuilder = parentQuery;
    }

    public HasParentQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Defines how the parent score is mapped into the child documents.
     */
    public HasParentQueryBuilder scoreType(String scoreType) {
        this.scoreType = scoreType;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasParentQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasParentQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("parent_type", parentType);
        if (scoreType != null) {
            builder.field("score_type", scoreType);
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }
}

