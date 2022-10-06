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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Parses and builds relevance_match queries
 */
public class RelevanceMatchQueryBuilder extends AbstractQueryBuilder<RelevanceMatchQueryBuilder> {

    public static final String NAME = "relevance_match";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField RELEVANCE_SETTINGS_FIELD = new ParseField("relevance_settings");
    private static final ParseField CURATIONS_SETTINGS_FIELD = new ParseField("curations");

    private static final ObjectParser<RelevanceMatchQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RelevanceMatchQueryBuilder::new);

    static {
        declareStandardFields(PARSER);

        PARSER.declareString(RelevanceMatchQueryBuilder::setQuery, QUERY_FIELD);
        PARSER.declareStringOrNull(RelevanceMatchQueryBuilder::setRelevanceSettingsId, RELEVANCE_SETTINGS_FIELD);
        PARSER.declareStringOrNull(RelevanceMatchQueryBuilder::setCurationsSettingsId, CURATIONS_SETTINGS_FIELD);
    }

    private String query;

    private String relevanceSettingsId;

    private String curationsSettingsId;

    private RelevanceMatchQueryRewriter queryRewriter;

    public RelevanceMatchQueryBuilder() {
        super();
    }

    public RelevanceMatchQueryBuilder(RelevanceMatchQueryRewriter queryRewriter) {
        super();
        this.queryRewriter = queryRewriter;
    }

    public RelevanceMatchQueryBuilder(RelevanceMatchQueryRewriter queryRewriter, StreamInput in) throws IOException {
        super(in);

        query = in.readString();
        relevanceSettingsId = in.readOptionalString();
        curationsSettingsId = in.readOptionalString();

        this.queryRewriter = queryRewriter;
    }

    public static RelevanceMatchQueryBuilder fromXContent(final XContentParser parser, RelevanceMatchQueryRewriter queryRewriter) {

        final RelevanceMatchQueryBuilder builder;
        try {
            builder = PARSER.apply(parser, null);
        } catch (final IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }

        if (builder.query == null) {
            throw new ParsingException(parser.getTokenLocation(), "[relevance_match] requires a query, none specified");
        }

        builder.setQueryRewriter(queryRewriter);

        return builder;
    }

    RelevanceMatchQueryRewriter getQueryRewriter() {
        return queryRewriter;
    }

    void setQueryRewriter(RelevanceMatchQueryRewriter queryRewriter) {
        this.queryRewriter = queryRewriter;
    }

    public void setCurationsSettingsId(String curationsSettingsId) {
        this.curationsSettingsId = curationsSettingsId;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(QUERY_FIELD.getPreferredName(), query);
        if (relevanceSettingsId != null) {
            builder.field(RELEVANCE_SETTINGS_FIELD.getPreferredName(), relevanceSettingsId);
        }
        if (curationsSettingsId != null) {
            builder.field(CURATIONS_SETTINGS_FIELD.getPreferredName(), curationsSettingsId);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public void setRelevanceSettingsId(String relevanceSettingsId) {
        this.relevanceSettingsId = relevanceSettingsId;
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeOptionalString(relevanceSettingsId);
        out.writeOptionalString(curationsSettingsId);
    }

    String getRelevanceSettingsId() {
        return relevanceSettingsId;
    }

    @Override
    protected Query doToQuery(final SearchExecutionContext context) throws IOException {
        return queryRewriter.rewriteQuery(this, context);
    }

    String getCurationsSettingsId() {
        return curationsSettingsId;
    }

    @Override
    protected boolean doEquals(final RelevanceMatchQueryBuilder other) {
        return Objects.equals(this.query, other.query)
            && Objects.equals(this.relevanceSettingsId, other.relevanceSettingsId)
            && Objects.equals(this.curationsSettingsId, other.curationsSettingsId);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, relevanceSettingsId, curationsSettingsId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_6_0;
    }

}
