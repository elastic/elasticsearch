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

import com.google.common.collect.Iterables;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends QueryBuilder implements Streamable, BoostableQueryBuilder<IdsQueryBuilder> {

    private List<String> types = new ArrayList<>();

    private List<String> ids = new ArrayList<>();

    private float boost = 1.0f;

    private String queryName;

    public IdsQueryBuilder() {
        //for serialization only
    }

    public IdsQueryBuilder(String... types) {
        this.types = (types == null || types.length == 0) ? new ArrayList<String>() : Arrays.asList(types);
    }

    /**
     * Get the types used in this query
     * @return the types
     */
    public Collection<String> types() {
        return this.types;
    }

    /**
     * Adds ids to the query.
     */
    public IdsQueryBuilder addIds(String... ids) {
        this.ids.addAll(Arrays.asList(ids));
        return this;
    }

    /**
     * Adds ids to the query.
     */
    public IdsQueryBuilder ids(String... ids) {
        return addIds(ids);
    }

    /**
     * Gets the ids for the query.
     */
    public Collection<String> ids() {
        return this.ids;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public IdsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Gets the boost for this query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the query that can be used when searching for matched_filters per hit.
     */
    public IdsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IdsQueryParser.NAME);
        if (types != null) {
            if (types.size() == 1) {
                builder.field("type", types.get(0));
            } else {
                builder.startArray("types");
                for (String type : types) {
                    builder.value(type);
                }
                builder.endArray();
            }
        }
        builder.startArray("values");
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    protected String parserName() {
        return IdsQueryParser.NAME;
    }

    public Query toQuery(QueryParseContext parseContext) throws IOException, QueryParsingException {
        if (this.ids.isEmpty()) {
            return Queries.newMatchNoDocsQuery();
        }

        Collection<String> typesForQuery = this.types;
        if (typesForQuery == null || typesForQuery.isEmpty()) {
            typesForQuery = parseContext.queryTypes();
        } else if (typesForQuery.size() == 1 && Iterables.getFirst(typesForQuery, null).equals("_all")) {
            typesForQuery = parseContext.mapperService().types();
        }

        TermsQuery query = new TermsQuery(UidFieldMapper.NAME, Uid.createUidsForTypesAndIds(typesForQuery, ids));
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        // all fields can be empty or null
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.types = in.readStringList();
        this.ids = in.readStringList();
        queryName = in.readOptionalString();
        boost = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringList(this.types);
        out.writeStringList(this.ids);
        out.writeOptionalString(queryName);
        out.writeFloat(boost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, types, boost, queryName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IdsQueryBuilder other = (IdsQueryBuilder) obj;
        return Objects.equals(ids, other.ids) &&
               Objects.equals(types, other.types) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }
}
