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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query that applies a filter to the results of another query.
 * @deprecated Use {@link BoolQueryBuilder} instead.
 */
@Deprecated
public class FilteredQueryBuilder extends AbstractQueryBuilder<FilteredQueryBuilder> {

    public static final String NAME = "filtered";

    private final QueryBuilder queryBuilder;

    private final QueryBuilder filterBuilder;

    static final FilteredQueryBuilder PROTOTYPE = new FilteredQueryBuilder(null, null);

    /**
     * A query that applies a filter to the results of another query.
     *
     * @param queryBuilder  The query to apply the filter to (Can be null)
     * @param filterBuilder The filter to apply on the query (Can be null)
     */
    public FilteredQueryBuilder(@Nullable QueryBuilder queryBuilder, @Nullable QueryBuilder filterBuilder) {
        this.queryBuilder = queryBuilder;
        this.filterBuilder = filterBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
