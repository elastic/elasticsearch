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

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends AbstractQueryBuilder<IdsQueryBuilder> {

    public static final String NAME = "ids";

    private final Set<String> ids = new HashSet<>();

    private final String[] types;

    static final IdsQueryBuilder PROTOTYPE = new IdsQueryBuilder();

    /**
     * Creates a new IdsQueryBuilder without providing the types of the documents to look for
     */
    public IdsQueryBuilder() {
        this.types = new String[0];
    }

    /**
     * Creates a new IdsQueryBuilder by providing the types of the documents to look for
     */
    public IdsQueryBuilder(String... types) {
        if (types == null) {
            throw new IllegalArgumentException("[ids] types cannot be null");
        }
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
        if (ids == null) {
            throw new IllegalArgumentException("[ids] ids cannot be null");
        }
        Collections.addAll(this.ids, ids);
        return this;
    }

    /**
     * Returns the ids for the query.
     */
    public Set<String> ids() {
        return this.ids;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.array(IdsQueryParser.TYPE_FIELD.getPreferredName(), types);
        builder.startArray(IdsQueryParser.VALUES_FIELD.getPreferredName());
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query;
        if (this.ids.isEmpty()) {
             query = Queries.newMatchNoDocsQuery();
        } else {
            Collection<String> typesForQuery;
            if (types.length == 0) {
                typesForQuery = context.queryTypes();
            } else if (types.length == 1 && MetaData.ALL.equals(types[0])) {
                typesForQuery = context.getMapperService().types();
            } else {
                typesForQuery = new HashSet<>();
                Collections.addAll(typesForQuery, types);
            }

            query = new TermsQuery(UidFieldMapper.NAME, Uid.createUidsForTypesAndIds(typesForQuery, ids));
        }
        return query;
    }

    @Override
    protected IdsQueryBuilder doReadFrom(StreamInput in) throws IOException {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder(in.readStringArray());
        idsQueryBuilder.addIds(in.readStringArray());
        return idsQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(types);
        out.writeStringArray(ids.toArray(new String[ids.size()]));
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(ids, Arrays.hashCode(types));
    }

    @Override
    protected boolean doEquals(IdsQueryBuilder other) {
        return Objects.equals(ids, other.ids) &&
               Arrays.equals(types, other.types);
    }
}
