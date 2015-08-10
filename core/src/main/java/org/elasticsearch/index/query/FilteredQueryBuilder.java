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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that applies a filter to the results of another query.
 * @deprecated Use {@link BoolQueryBuilder} instead.
 */
@Deprecated
public class FilteredQueryBuilder extends AbstractQueryBuilder<FilteredQueryBuilder> {

    /** Name of the query in the REST API. */
    public static final String NAME = "filtered";
    /** The query to filter. */
    private final QueryBuilder queryBuilder;
    /** The filter to apply to the query. */
    private final QueryBuilder filterBuilder;

    static final FilteredQueryBuilder PROTOTYPE = new FilteredQueryBuilder(null, null);

    /**
     * Returns a {@link MatchAllQueryBuilder} instance that will be used as
     * default queryBuilder if none is supplied by the user. Feel free to
     * set queryName and boost on that instance - it's always a new one.
     * */
    private static QueryBuilder generateDefaultQuery() {
        return new MatchAllQueryBuilder();
    }

    /**
     * A query that applies a filter to the results of a match_all query.
     * @param filterBuilder The filter to apply on the query (Can be null)
     * */
    public FilteredQueryBuilder(QueryBuilder filterBuilder) {
        this(generateDefaultQuery(), filterBuilder);
    }

    /**
     * A query that applies a filter to the results of another query.
     *
     * @param queryBuilder  The query to apply the filter to
     * @param filterBuilder The filter to apply on the query (Can be null)
     */
    public FilteredQueryBuilder(QueryBuilder queryBuilder, QueryBuilder filterBuilder) {
        this.queryBuilder = (queryBuilder != null) ? queryBuilder : generateDefaultQuery();
        this.filterBuilder = (filterBuilder != null) ? filterBuilder : EmptyQueryBuilder.PROTOTYPE;
    }

    /** Returns the query to apply the filter to. */
    public QueryBuilder query() {
        return queryBuilder;
    }

    /** Returns the filter to apply to the query results. */
    public QueryBuilder filter() {
        return filterBuilder;
    }

    @Override
    protected boolean doEquals(FilteredQueryBuilder other) {
        return Objects.equals(queryBuilder, other.queryBuilder) &&
                Objects.equals(filterBuilder, other.filterBuilder);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(queryBuilder, filterBuilder);
    }

    @Override
    public Query doToQuery(QueryShardContext context) throws QueryShardException, IOException {
        Query query = queryBuilder.toQuery(context);
        Query filter = filterBuilder.toFilter(context);

        if (query == null) {
            // Most likely this query was generated from the JSON query DSL - it parsed to an EmptyQueryBuilder so we ignore
            // the whole filtered query as there is nothing to filter on. See FilteredQueryParser for an example.
            return null;
        }

        if (filter == null || Queries.isConstantMatchAllQuery(filter)) {
            // no filter, or match all filter
            return query;
        } else if (Queries.isConstantMatchAllQuery(query)) {
            // if its a match_all query, use constant_score
            return new ConstantScoreQuery(filter);
        }

        // use a BooleanQuery
        return Queries.filtered(query, filter);
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        validationException = validateInnerQuery(queryBuilder, validationException);
        validationException = validateInnerQuery(filterBuilder, validationException);
        return validationException;

    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("filter");
        filterBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public FilteredQueryBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder query = in.readQuery();
        QueryBuilder filter = in.readQuery();
        FilteredQueryBuilder qb = new FilteredQueryBuilder(query, filter);
        return qb;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(queryBuilder);
        out.writeQuery(filterBuilder);
    }
}
