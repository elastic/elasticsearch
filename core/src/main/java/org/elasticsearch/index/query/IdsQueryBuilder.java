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

import com.google.common.collect.Sets;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.*;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends AbstractQueryBuilder<IdsQueryBuilder> implements BoostableQueryBuilder<IdsQueryBuilder> {

    public static final String NAME = "ids";

    private final Set<String> ids = Sets.newHashSet();

    private final String[] types;

    private float boost = 1.0f;

    private String queryName;

    static final IdsQueryBuilder PROTOTYPE = new IdsQueryBuilder();

    /**
     * Creates a new IdsQueryBuilder by optionally providing the types of the documents to look for
     */
    public IdsQueryBuilder(@Nullable String... types) {
        this.types = types;
    }

    /**
     * Returns the types used in this query
     */
    public String[] types() {
        return this.types;
    }

    /**
     * Adds ids to the query.
     */
    public IdsQueryBuilder addIds(String... ids) {
        Collections.addAll(this.ids, ids);
        return this;
    }

    /**
     * Adds ids to the query.
     */
    public IdsQueryBuilder addIds(Collection<String> ids) {
        this.ids.addAll(ids);
        return this;
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder ids(String... ids) {
        return addIds(ids);
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder ids(Collection<String> ids) {
        return addIds(ids);
    }

    /**
     * Returns the ids for the query.
     */
    public Set<String> ids() {
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
        builder.startObject(NAME);
        if (types != null) {
            if (types.length == 1) {
                builder.field("type", types[0]);
            } else {
                builder.array("types", types);
            }
        }
        builder.startArray("values");
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws IOException, QueryParsingException {
        Query query;
        if (this.ids.isEmpty()) {
             query = Queries.newMatchNoDocsQuery();
        } else {
            Collection<String> typesForQuery;
            if (types == null || types.length == 0) {
                typesForQuery = parseContext.queryTypes();
            } else if (types.length == 1 && MetaData.ALL.equals(types[0])) {
                typesForQuery = parseContext.mapperService().types();
            } else {
                typesForQuery = Sets.newHashSet(types);
            }

            query = new TermsQuery(UidFieldMapper.NAME, Uid.createUidsForTypesAndIds(typesForQuery, ids));
        }
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
    public IdsQueryBuilder readFrom(StreamInput in) throws IOException {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder(in.readStringArray());
        idsQueryBuilder.addIds(in.readStringArray());
        idsQueryBuilder.queryName = in.readOptionalString();
        idsQueryBuilder.boost = in.readFloat();
        return idsQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(this.types);
        out.writeStringArray(this.ids.toArray(new String[this.ids.size()]));
        out.writeOptionalString(queryName);
        out.writeFloat(boost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, Arrays.hashCode(types), boost, queryName);
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
               Arrays.equals(types, other.types) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }
}
