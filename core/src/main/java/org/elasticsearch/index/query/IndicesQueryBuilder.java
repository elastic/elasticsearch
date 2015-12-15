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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A query that will execute the wrapped query only for the specified indices, and "match_all" when
 * it does not match those indices (by default).
 */
public class IndicesQueryBuilder extends AbstractQueryBuilder<IndicesQueryBuilder> {

    public static final String NAME = "indices";

    private final QueryBuilder innerQuery;

    private final String[] indices;

    private QueryBuilder noMatchQuery = defaultNoMatchQuery();

    static final IndicesQueryBuilder PROTOTYPE = new IndicesQueryBuilder(EmptyQueryBuilder.PROTOTYPE, "index");

    public IndicesQueryBuilder(QueryBuilder innerQuery, String... indices) {
        if (innerQuery == null) {
            throw new IllegalArgumentException("inner query cannot be null");
        }
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("list of indices cannot be null or empty");
        }
        this.innerQuery = Objects.requireNonNull(innerQuery);
        this.indices = indices;
    }

    public QueryBuilder innerQuery() {
        return this.innerQuery;
    }

    public String[] indices() {
        return this.indices;
    }

    /**
     * Sets the query to use when it executes on an index that does not match the indices provided.
     */
    public IndicesQueryBuilder noMatchQuery(QueryBuilder noMatchQuery) {
        if (noMatchQuery == null) {
            throw new IllegalArgumentException("noMatch query cannot be null");
        }
        this.noMatchQuery = noMatchQuery;
        return this;
    }

    /**
     * Sets the no match query, can either be <tt>all</tt> or <tt>none</tt>.
     */
    public IndicesQueryBuilder noMatchQuery(String type) {
        this.noMatchQuery = IndicesQueryParser.parseNoMatchQuery(type);
        return this;
    }

    public QueryBuilder noMatchQuery() {
        return this.noMatchQuery;
    }

    static QueryBuilder defaultNoMatchQuery() {
        return QueryBuilders.matchAllQuery();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(IndicesQueryParser.INDICES_FIELD.getPreferredName(), indices);
        builder.field(IndicesQueryParser.QUERY_FIELD.getPreferredName());
        innerQuery.toXContent(builder, params);
        builder.field(IndicesQueryParser.NO_MATCH_QUERY.getPreferredName());
        noMatchQuery.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.matchesIndices(indices)) {
            return innerQuery.toQuery(context);
        }
        return noMatchQuery.toQuery(context);
    }

    @Override
    protected IndicesQueryBuilder doReadFrom(StreamInput in) throws IOException {
        IndicesQueryBuilder indicesQueryBuilder = new IndicesQueryBuilder(in.readQuery(), in.readStringArray());
        indicesQueryBuilder.noMatchQuery = in.readQuery();
        return indicesQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(innerQuery);
        out.writeStringArray(indices);
        out.writeQuery(noMatchQuery);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(innerQuery, noMatchQuery, Arrays.hashCode(indices));
    }

    @Override
    protected boolean doEquals(IndicesQueryBuilder other) {
        return Objects.equals(innerQuery, other.innerQuery) &&
                Arrays.equals(indices, other.indices) &&  // otherwise we are comparing pointers
                Objects.equals(noMatchQuery, other.noMatchQuery);
    }
}
