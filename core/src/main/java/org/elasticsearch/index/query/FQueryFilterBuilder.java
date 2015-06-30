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
 * A filter that simply wraps a query. Same as the {@link QueryFilterBuilder} except that it allows also to
 * associate a name with the query filter.
 * @deprecated Useless now that queries and filters are merged: pass the
 *             query as a filter directly.
 */
@Deprecated
public class FQueryFilterBuilder extends AbstractQueryBuilder<FQueryFilterBuilder> {

    public static final String NAME = "fquery";

    static final FQueryFilterBuilder PROTOTYPE = new FQueryFilterBuilder(null);

    private final QueryBuilder queryBuilder;

    /**
     * A filter that simply wraps a query.
     *
     * @param queryBuilder The query to wrap as a filter
     */
    public FQueryFilterBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * @return the query builder that is wrapped by this {@link FQueryFilterBuilder}
     */
    public QueryBuilder innerQuery() {
        return this.queryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FQueryFilterBuilder.NAME);
        doXContentInnerBuilder(builder, "query", queryBuilder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        // inner query builder can potentially be `null`, in that case we ignore it
        if (this.queryBuilder == null) {
            return null;
        }
        Query innerQuery = this.queryBuilder.toQuery(parseContext);
        if (innerQuery == null) {
            return null;
        }
        return new ConstantScoreQuery(innerQuery);
    }

    @Override
    public QueryValidationException validate() {
        return validateInnerQuery(queryBuilder, null);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryBuilder);
    }

    @Override
    protected boolean doEquals(FQueryFilterBuilder other) {
        return Objects.equals(queryBuilder, other.queryBuilder);
    }

    @Override
    protected FQueryFilterBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder innerQueryBuilder = in.readNamedWriteable();
        FQueryFilterBuilder fquery = new FQueryFilterBuilder(innerQueryBuilder);
        return fquery;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
