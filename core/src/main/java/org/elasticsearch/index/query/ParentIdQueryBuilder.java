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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public final class ParentIdQueryBuilder extends AbstractQueryBuilder<ParentIdQueryBuilder> {

    public static final String NAME = "parent_id";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField TYPE_FIELD = new ParseField("type", "child_type");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String type;
    private final String id;

    private boolean ignoreUnmapped = false;

    public ParentIdQueryBuilder(String type, String id) {
        this.type = type;
        this.id = id;
    }

    /**
     * Read from a stream.
     */
    public ParentIdQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        id = in.readString();
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(id);
        out.writeBoolean(ignoreUnmapped);
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public ParentIdQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<ParentIdQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String type = null;
        String id = null;
        String queryName = null;
        String currentFieldName = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    type = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ID_FIELD)) {
                    id = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[parent_id] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[parent_id] query does not support [" + currentFieldName + "]");
            }
        }
        ParentIdQueryBuilder queryBuilder = new ParentIdQueryBuilder(type, id);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        queryBuilder.ignoreUnmapped(ignoreUnmapped);
        return Optional.of(queryBuilder);
    }


    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        DocumentMapper childDocMapper = context.getMapperService().documentMapper(type);
        if (childDocMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] no mapping found for type [" + type + "]");
            }
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (parentFieldMapper.active() == false) {
            throw new QueryShardException(context, "[" + NAME + "] _parent field has no parent type configured");
        }
        String fieldName = ParentFieldMapper.joinField(parentFieldMapper.type());

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new DocValuesTermsQuery(fieldName, id), BooleanClause.Occur.MUST);
        // Need to take child type into account, otherwise a child doc of different type with the same id could match
        query.add(new TermQuery(new Term(TypeFieldMapper.NAME, type)), BooleanClause.Occur.FILTER);
        return query.build();
    }

    @Override
    protected boolean doEquals(ParentIdQueryBuilder that) {
        return Objects.equals(type, that.type)
                && Objects.equals(id, that.id)
                && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(type, id, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
