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
public class ConstantScoreQueryBuilder extends AbstractQueryBuilder<ConstantScoreQueryBuilder> implements BoostableQueryBuilder<ConstantScoreQueryBuilder> {

    public static final String NAME = "constant_score";

    private final QueryBuilder filterBuilder;

    private float boost = 1.0f;

    static final ConstantScoreQueryBuilder PROTOTYPE = new ConstantScoreQueryBuilder(null);

    /**
     * A query that wraps another query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param filterBuilder The query to wrap in a constant score query
     */
    public ConstantScoreQueryBuilder(QueryBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
    }

    /**
     * @return the query that was wrapped in this constant score query
     */
    public QueryBuilder query() {
        return this.filterBuilder;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public ConstantScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * @return the boost factor
     */
    public float boost() {
        return this.boost;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("filter");
        filterBuilder.toXContent(builder, params);
        builder.field("boost", boost);
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        // current DSL allows empty inner filter clauses, we ignore them
        if (filterBuilder == null) {
            return null;
        }

        Query innerFilter = filterBuilder.toQuery(parseContext);
        if (innerFilter == null ) {
            // return null so that parent queries (e.g. bool) also ignore this
            return null;
        }

        Query filter = new ConstantScoreQuery(filterBuilder.toQuery(parseContext));
        filter.setBoost(boost);
        return filter;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(boost, filterBuilder);
    }

    @Override
    public boolean doEquals(ConstantScoreQueryBuilder other) {
        return Objects.equals(boost, other.boost) &&
                Objects.equals(filterBuilder, other.filterBuilder);
    }

    @Override
    public ConstantScoreQueryBuilder readFrom(StreamInput in) throws IOException {
        QueryBuilder innerFilterBuilder = in.readNamedWriteable();
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(innerFilterBuilder);
        constantScoreQueryBuilder.boost = in.readFloat();
        return constantScoreQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(this.filterBuilder);
        out.writeFloat(boost);
    }
}
