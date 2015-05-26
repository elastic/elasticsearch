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
 * A filter that simply wraps a query.
 * @deprecated Useless now that queries and filters are merged: pass the
 *             query as a filter directly.
 */
@Deprecated
public class QueryFilterBuilder extends QueryBuilder {

    public static final String NAME = "query";

    // this query builder creates query parsed by FQueryFilterParser in case queryName is set
    public static final String FQUERY_NAME = "fquery";

    private final QueryBuilder queryBuilder;

    private String queryName;

    static final QueryFilterBuilder PROTOTYPE = new QueryFilterBuilder(null);

    /**
     * A filter that simply wraps a query.
     *
     * @param queryBuilder The query to wrap as a filter
     */
    public QueryFilterBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public QueryFilterBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryName == null) {
            builder.field(NAME);
            queryBuilder.toXContent(builder, params);
        } else {
            builder.startObject(FQUERY_NAME);
            builder.field("query");
            queryBuilder.toXContent(builder, params);
            if (queryName != null) {
                builder.field("_name", queryName);
            }
            builder.endObject();
        }
    }

    @Override
    public String queryId() {
        return NAME;
    }
}
