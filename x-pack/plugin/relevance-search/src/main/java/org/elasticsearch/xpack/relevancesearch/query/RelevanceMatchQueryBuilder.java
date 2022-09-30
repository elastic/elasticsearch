/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Parses and builds relevance_match queries
 */
public class RelevanceMatchQueryBuilder extends AbstractQueryBuilder<RelevanceMatchQueryBuilder> {

    public static final String NAME = "relevance_match";

    private static final ParseField FIELD_QUERY = new ParseField("query");

    private static final ObjectParser<RelevanceMatchQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RelevanceMatchQueryBuilder::new);

    static {
        declareStandardFields(PARSER);

        PARSER.declareString(RelevanceMatchQueryBuilder::setQuery, FIELD_QUERY);
    }

    private String query;

    public RelevanceMatchQueryBuilder() {
        super();
    }

    public RelevanceMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);

        query = in.readString();
    }

    public static RelevanceMatchQueryBuilder fromXContent(final XContentParser parser) {

        final RelevanceMatchQueryBuilder builder;
        try {
            builder = PARSER.apply(parser, null);
        } catch (final IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }

        if (builder.query == null) {
            throw new ParsingException(parser.getTokenLocation(), "[relevance_match] requires a query, none specified");
        }
        return builder;
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        out.writeString(query);
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(FIELD_QUERY.getPreferredName(), query);

        builder.endObject();
    }

    @Override
    protected Query doToQuery(final SearchExecutionContext context) throws IOException {
        Collection<String> fields = QueryFieldsResolver.getQueryFields(context);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("[relevance_match] query cannot find text fields in the index");
        }

        final CombinedFieldsQueryBuilder builder = new CombinedFieldsQueryBuilder(query, fields.toArray(new String[fields.size()]));

        return builder.toQuery(context);
    }

    @Override
    protected boolean doEquals(final RelevanceMatchQueryBuilder other) {
        return Objects.equals(this.query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_6_0;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
