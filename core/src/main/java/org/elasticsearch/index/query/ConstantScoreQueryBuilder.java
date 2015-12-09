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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 */
public class ConstantScoreQueryBuilder extends AbstractQueryBuilder<ConstantScoreQueryBuilder> {

    public static final String NAME = "constant_score";

    private final QueryBuilder filterBuilder;

    static final ConstantScoreQueryBuilder PROTOTYPE = new ConstantScoreQueryBuilder(EmptyQueryBuilder.PROTOTYPE);

    /**
     * A query that wraps another query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param filterBuilder The query to wrap in a constant score query
     */
    public ConstantScoreQueryBuilder(QueryBuilder filterBuilder) {
        if (filterBuilder == null) {
            throw new IllegalArgumentException("inner clause [filter] cannot be null.");
        }
        this.filterBuilder = filterBuilder;
    }

    /**
     * @return the query that was wrapped in this constant score query
     */
    public QueryBuilder innerQuery() {
        return this.filterBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ConstantScoreQueryParser.INNER_QUERY_FIELD.getPreferredName());
        filterBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerFilter = filterBuilder.toFilter(context);
        if (innerFilter == null ) {
            // return null so that parent queries (e.g. bool) also ignore this
            return null;
        }
        return new ConstantScoreQuery(innerFilter);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filterBuilder);
    }

    @Override
    protected boolean doEquals(ConstantScoreQueryBuilder other) {
        return Objects.equals(filterBuilder, other.filterBuilder);
    }

    @Override
    protected ConstantScoreQueryBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder innerFilterBuilder = in.readQuery();
        return new ConstantScoreQueryBuilder(innerFilterBuilder);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(filterBuilder);
    }
}
