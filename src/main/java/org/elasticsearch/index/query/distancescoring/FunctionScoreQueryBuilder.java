/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query.distancescoring;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

public class FunctionScoreQueryBuilder extends BaseQueryBuilder {

    private final QueryBuilder queryBuilder;

    private ScoreFunctionBuilder scoreBuilder;

    /**
     * A query that multiplies the score computed by another query with a
     * function centered about a user given reference. The farther the numeric
     * values of the document are from the user given reference, the lower the
     * score is weighted.
     * 
     * @param queryBuilder
     *            The query to apply the boost factor to.
     * @param scoreBuilder
     */
    public FunctionScoreQueryBuilder(QueryBuilder queryBuilder, ScoreFunctionBuilder scoreBuilder) {
        this.queryBuilder = queryBuilder;
        this.scoreBuilder = scoreBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FunctionScoreQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field(((ScoreFunctionBuilder) scoreBuilder).getName());
        scoreBuilder.toXContent(builder, params);
        builder.endObject();
    }
}