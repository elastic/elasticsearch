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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 */
public class NotQueryBuilder extends AbstractQueryBuilder<NotQueryBuilder> {

    public static final String NAME = "not";

    private final QueryBuilder filter;

    private String queryName;

    static final NotQueryBuilder PROTOTYPE = new NotQueryBuilder(null);

    public NotQueryBuilder(QueryBuilder filter) {
        this.filter = filter;
    }

    /**
     * @return the filter added to "not".
     */
    public QueryBuilder filter() {
        return this.filter;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public NotQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * @return the query name.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        filter.toXContent(builder, params);
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        if (filter == null) {
            return null;
        }

        Query luceneQuery = filter.toQuery(parseContext);
        if (luceneQuery == null) {
            return null;
        }

        Query notQuery = Queries.not(luceneQuery);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, notQuery);
        }
        return notQuery;
    }

    @Override
    public QueryValidationException validate() {
        // nothing to validate.
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, queryName);
    }

    @Override
    public boolean doEquals(NotQueryBuilder other) {
        return Objects.equals(filter, other.filter) &&
               Objects.equals(queryName, other.queryName);
    }

    @Override
    public NotQueryBuilder readFrom(StreamInput in) throws IOException {
        QueryBuilder queryBuilder = in.readNamedWriteable();
        NotQueryBuilder notQueryBuilder = new NotQueryBuilder(queryBuilder);
        notQueryBuilder.queryName = in.readOptionalString();
        return notQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
        out.writeOptionalString(queryName);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
