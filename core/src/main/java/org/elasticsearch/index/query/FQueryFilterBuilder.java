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

    private String queryName;

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

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FQueryFilterBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * @return the query name for the filter that can be used when searching for matched_filters per hit
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FQueryFilterBuilder.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        // inner query builder can potentially be `null`, in that case we ignore it
        if (this.queryBuilder == null) {
            return null;
        }
        Query innerQuery = this.queryBuilder.toQuery(parseContext);
        if (innerQuery == null) {
            return null;
        }
        Query query = new ConstantScoreQuery(innerQuery);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilder, queryName);
    }

    @Override
    public boolean doEquals(FQueryFilterBuilder other) {
        return Objects.equals(queryBuilder, other.queryBuilder) &&
                Objects.equals(queryName, other.queryName);
    }

    @Override
    public FQueryFilterBuilder readFrom(StreamInput in) throws IOException {
        QueryBuilder innerQueryBuilder = in.readNamedWriteable();
        FQueryFilterBuilder fquery = new FQueryFilterBuilder(innerQueryBuilder);
        fquery.queryName = in.readOptionalString();
        return fquery;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(this.queryBuilder);
        out.writeOptionalString(queryName);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
