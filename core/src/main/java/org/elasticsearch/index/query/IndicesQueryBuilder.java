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
 * A query that will execute the wrapped query only for the specified indices, and "match_all" when
 * it does not match those indices (by default).
 */
public class IndicesQueryBuilder extends QueryBuilder {

    private final QueryBuilder queryBuilder;

    private final String[] indices;

    private String sNoMatchQuery;
    private QueryBuilder noMatchQuery;

    private String queryName;

    public IndicesQueryBuilder(QueryBuilder queryBuilder, String... indices) {
        this.queryBuilder = queryBuilder;
        this.indices = indices;
    }

    /**
     * Sets the no match query, can either be <tt>all</tt> or <tt>none</tt>.
     */
    public IndicesQueryBuilder noMatchQuery(String type) {
        this.sNoMatchQuery = type;
        return this;
    }

    /**
     * Sets the query to use when it executes on an index that does not match the indices provided.
     */
    public IndicesQueryBuilder noMatchQuery(QueryBuilder noMatchQuery) {
        this.noMatchQuery = noMatchQuery;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public IndicesQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IndicesQueryParser.NAME);
        builder.field("indices", indices);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        if (noMatchQuery != null) {
            builder.field("no_match_query");
            noMatchQuery.toXContent(builder, params);
        } else if (sNoMatchQuery != null) {
            builder.field("no_match_query", sNoMatchQuery);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}