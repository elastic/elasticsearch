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

import com.google.common.collect.Lists;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 * @deprecated Use {@link BoolQueryBuilder} instead
 */
@Deprecated
public class OrQueryBuilder extends AbstractQueryBuilder<OrQueryBuilder> {

    public static final String NAME = "or";

    private ArrayList<QueryBuilder> filters = Lists.newArrayList();

    private String queryName;

    static final OrQueryBuilder PROTOTYPE = new OrQueryBuilder();

    public OrQueryBuilder(QueryBuilder... filters) {
        Collections.addAll(this.filters, filters);
    }

    /**
     * Adds a filter to the list of filters to "or".
     */
    public OrQueryBuilder add(QueryBuilder filterBuilder) {
        filters.add(filterBuilder);
        return this;
    }

    /**
     * @return the list of filters added to "or".
     */
    public List<QueryBuilder> filters() {
        return this.filters;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public OrQueryBuilder queryName(String queryName) {
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
        builder.startArray("filters");
        for (QueryBuilder filter : filters) {
            filter.toXContent(builder, params);
        }
        builder.endArray();
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        if (filters.isEmpty()) {
            // no filters provided, this should be ignored upstream
            return null;
        }

        BooleanQuery query = new BooleanQuery();
        for (QueryBuilder f : filters) {
            Query innerQuery = f.toQuery(parseContext);
            // ignore queries that are null
            if (innerQuery != null) {
                query.add(innerQuery, Occur.SHOULD);
            }
        }
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        // nothing to validate.
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filters, queryName);
    }

    @Override
    public boolean doEquals(OrQueryBuilder other) {
        return Objects.equals(filters, other.filters) &&
               Objects.equals(queryName, other.queryName);
    }

    @Override
    public OrQueryBuilder readFrom(StreamInput in) throws IOException {
        OrQueryBuilder orQueryBuilder = new OrQueryBuilder();
        List<QueryBuilder> queryBuilders = in.readNamedWritableList();
        for (QueryBuilder queryBuilder : queryBuilders) {
            orQueryBuilder.add(queryBuilder);
        }
        orQueryBuilder.queryName = in.readOptionalString();
        return orQueryBuilder;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWritableList(this.filters);
        out.writeOptionalString(queryName);
    }
}
