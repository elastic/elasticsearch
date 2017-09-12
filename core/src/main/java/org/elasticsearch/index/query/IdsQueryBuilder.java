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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ObjectParser.fromList;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends AbstractQueryBuilder<IdsQueryBuilder> {
    public static final String NAME = "ids";

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField VALUES_FIELD = new ParseField("values");

    private final Set<String> ids = new HashSet<>();

    private String[] types = Strings.EMPTY_ARRAY;

    /**
     * Creates a new IdsQueryBuilder with no types specified upfront
     */
    public IdsQueryBuilder() {
        // nothing to do
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
     * Add types to query
     */
    public IdsQueryBuilder types(String... types) {
        if (types == null) {
            throw new IllegalArgumentException("[" + NAME + "] types cannot be null");
        }
        this.types = types;
        return this;
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
            throw new IllegalArgumentException("[" + NAME + "] ids cannot be null");
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

    private static ObjectParser<IdsQueryBuilder, Void> PARSER = new ObjectParser<>(NAME,
            () -> new IdsQueryBuilder());

    static {
        PARSER.declareStringArray(fromList(String.class, IdsQueryBuilder::types), IdsQueryBuilder.TYPE_FIELD);
        PARSER.declareStringArray(fromList(String.class, IdsQueryBuilder::addIds), IdsQueryBuilder.VALUES_FIELD);
        declareStandardFields(PARSER);
    }

    public static IdsQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query;
        MappedFieldType uidField = context.fieldMapper(UidFieldMapper.NAME);
        if (uidField == null) {
            return new MatchNoDocsQuery("No mappings");
        }
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

            query = uidField.termsQuery(Arrays.asList(Uid.createUidsForTypesAndIds(typesForQuery, ids)), context);
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
