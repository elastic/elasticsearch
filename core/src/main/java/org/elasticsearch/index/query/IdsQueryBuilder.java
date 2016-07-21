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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends AbstractQueryBuilder<IdsQueryBuilder> {
    public static final String NAME = "ids";

    private static final ParseField TYPE_FIELD = new ParseField("type", "types", "_type");
    private static final ParseField VALUES_FIELD = new ParseField("values");

    private final Set<String> ids = new HashSet<>();

    private final String[] types;

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
     * Read from a stream.
     */
    public IdsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        types = in.readStringArray();
        Collections.addAll(ids, in.readStringArray());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(types);
        out.writeStringArray(ids.toArray(new String[ids.size()]));
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
        builder.array(TYPE_FIELD.getPreferredName(), types);
        builder.startArray(VALUES_FIELD.getPreferredName());
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<IdsQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        List<String> ids = new ArrayList<>();
        List<String> types = new ArrayList<>();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        boolean idsProvided = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, VALUES_FIELD)) {
                    idsProvided = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if ((token == XContentParser.Token.VALUE_STRING) ||
                                (token == XContentParser.Token.VALUE_NUMBER)) {
                            String id = parser.textOrNull();
                            if (id == null) {
                                throw new ParsingException(parser.getTokenLocation(), "No value specified for term filter");
                            }
                            ids.add(id);
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "Illegal value for id, expecting a string or number, got: " + token);
                        }
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new ParsingException(parser.getTokenLocation(), "No type specified for term filter");
                        }
                        types.add(value);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + IdsQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    types = Collections.singletonList(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + IdsQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + IdsQueryBuilder.NAME +
                        "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }
        if (!idsProvided) {
            throw new ParsingException(parser.getTokenLocation(), "[" + IdsQueryBuilder.NAME + "] query, no ids values provided");
        }

        IdsQueryBuilder query = new IdsQueryBuilder(types.toArray(new String[types.size()]));
        query.addIds(ids.toArray(new String[ids.size()]));
        query.boost(boost).queryName(queryName);
        return Optional.of(query);
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query;
        if (this.ids.isEmpty()) {
             query = Queries.newMatchNoDocsQuery("Missing ids in \"" + this.getName() + "\" query.");
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
    protected int doHashCode() {
        return Objects.hash(ids, Arrays.hashCode(types));
    }

    @Override
    protected boolean doEquals(IdsQueryBuilder other) {
        return Objects.equals(ids, other.ids) &&
               Arrays.equals(types, other.types);
    }
}
