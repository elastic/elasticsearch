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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<IdsQueryBuilder>, Streamable {

    private Collection<String> types = new ArrayList<>();

    private Collection<String> values = new ArrayList<>();

    private float boost = 1.0f;

    private String queryName;

    public static final String NAME = "ids";

    @Inject
    public IdsQueryBuilder() {
    }

    public IdsQueryBuilder(String... types) {
        this.types = types == null ? new ArrayList<String>() : Arrays.asList(types);
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder addIds(String... ids) {
        values.addAll(Arrays.asList(ids));
        return this;
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder ids(String... ids) {
        return addIds(ids);
    }

    Collection<String> ids() {
        return this.values;
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

    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public IdsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IdsQueryBuilder.NAME);
        if (types != null) {
            if (types.size() == 1) {
                builder.field("type", types.iterator().next());
            } else {
                builder.startArray("types");
                for (Object type : types) {
                    builder.value(type);
                }
                builder.endArray();
            }
        }
        builder.startArray("values");
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();
        if (boost !=  1.0f) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        IdsQueryBuilder query = new IdsQueryBuilder();
        query.fromXContent(parseContext);
        return query.toQuery(parseContext);
    }

    public void fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        XContentParser.Token token;
        boolean idsProvided = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("values".equals(currentFieldName)) {
                    idsProvided = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if ((token == XContentParser.Token.VALUE_STRING) ||
                                (token == XContentParser.Token.VALUE_NUMBER)) {
                            String id = parser.textOrNull();
                            if (id == null) {
                                throw new QueryParsingException(parseContext.index(), "No value specified for term filter");
                            }
                            values.add(id);
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "Illegal value for id, expecting a string or number, got: " + token);
                        }
                    }
                } else if ("types".equals(currentFieldName) || "type".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "No type specified for term filter");
                        }
                        types.add(value);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[ids] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "_type".equals(currentFieldName)) {
                    types = ImmutableList.of(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[ids] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!idsProvided) {
            throw new QueryParsingException(parseContext.index(), "[ids] query, no ids values provided");
        }
    }

    public Query toQuery(QueryParseContext parseContext) throws IOException, QueryParsingException {
        ArrayList<BytesRef> ids = new ArrayList<BytesRef>();

        for (String value : this.values) {
            BytesRef ref = new BytesRef(value);
            ids.add(ref);
        }

        if (ids.isEmpty()) {
            return Queries.newMatchNoDocsQuery();
        }

        if (types == null || types.isEmpty()) {
            types = parseContext.queryTypes();
        } else if (types.size() == 1 && Iterables.getFirst(types, null).equals("_all")) {
            types = parseContext.mapperService().types();
        }

        TermsFilter filter = new TermsFilter(UidFieldMapper.NAME, Uid.createTypeUids(types, ids));
        // no need for constant score filter, since we don't cache the filter, and it always takes deletes into account
        ConstantScoreQuery query = new ConstantScoreQuery(filter);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int typeSize = in.readInt();
        for (int i = 0; i < typeSize; i++) {
            types.add(in.readString());
        }
        int valueSize = in.readInt();
        for (int i = 0; i < valueSize; i++) {
            values.add(in.readString());
        }
        queryName = in.readOptionalString();
        boost = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(types.size());
        for (String type : types) {
            out.writeString(type);
        }
        out.writeInt(values.size());
        for (String value : values) {
            out.writeString(value);
        }
        out.writeOptionalString(queryName);
        out.writeFloat(boost);
    }

    @Override
    public int hashCode() {
        int hash = maybeHashcode(1, values);
        hash = maybeHashcode(hash, types);
        hash = maybeHashcode(hash, queryName);
        hash = maybeHashcode(hash, boost);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IdsQueryBuilder other = (IdsQueryBuilder) obj;
        return Objects.equals(values, other.values) &&
               Objects.equals(types, other.types) &&
               Objects.equals(boost, other.boost)&&
               Objects.equals(queryName, other.queryName);
    }

    /**
     * Return a prime (31) times the staring hash and object's hash, if non-null
     */
    private int maybeHashcode(int startingHash, Object obj) {
        return 31 * startingHash + ((obj == null) ? 0 : obj.hashCode());
    }
}
