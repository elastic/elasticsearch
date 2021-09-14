/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.join.mapper.Joiner;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public final class ParentIdQueryBuilder extends AbstractQueryBuilder<ParentIdQueryBuilder> {
    public static final String NAME = "parent_id";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField TYPE_FIELD = new ParseField("type");
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

    public static ParentIdQueryBuilder fromXContent(XContentParser parser) throws IOException {
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
                if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = parser.text();
                } else if (ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    id = parser.text();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
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
        return queryBuilder;
    }


    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[joining] queries cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }

        Joiner joiner = Joiner.getJoiner(context);
        if (joiner == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                final String indexName = context.getIndexSettings().getIndex().getName();
                throw new QueryShardException(context, "[" + NAME + "] no join field found for index [" + indexName  + "]");
            }
        }
        if (joiner.childTypeExists(type) == false) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] no relation found for child [" + type + "]");
            }
        }
        return new BooleanQuery.Builder()
            .add(new TermQuery(new Term(joiner.parentJoinField(type), id)), BooleanClause.Occur.MUST)
            // Need to take child type into account, otherwise a child doc of different type with the same id could match
            .add(new TermQuery(new Term(joiner.getJoinField(), type)), BooleanClause.Occur.FILTER)
            .build();
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
