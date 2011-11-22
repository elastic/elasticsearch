/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 * it does not match those indices.
 */
public class IndicesQueryBuilder extends BaseQueryBuilder {

    private final QueryBuilder queryBuilder;

    private final String[] indices;

    public IndicesQueryBuilder(QueryBuilder queryBuilder, String... indices) {
        this.queryBuilder = queryBuilder;
        this.indices = indices;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IndicesQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("indices", indices);
        builder.endObject();
    }
}